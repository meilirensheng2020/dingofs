/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Date: Sat Aug  7 22:46:58 CST 2021
 * Author: wuhanqing
 */

#include "metaserver/copyset/meta_operator.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "dingofs/metaserver.pb.h"
#include "common/rpc_stream.h"
#include "metaserver/copyset/meta_operator_closure.h"
#include "metaserver/copyset/raft_log_codec.h"
#include "metaserver/metastore.h"
#include "metaserver/streaming_utils.h"
#include "utils/timeutility.h"

static bvar::LatencyRecorder g_concurrent_fast_apply_wait_latency(
    "concurrent_fast_apply_wait");

namespace dingofs {
namespace metaserver {
namespace copyset {

using pb::metaserver::BatchGetInodeAttrRequest;
using pb::metaserver::BatchGetInodeAttrResponse;
using pb::metaserver::BatchGetXAttrRequest;
using pb::metaserver::BatchGetXAttrResponse;
using pb::metaserver::CreateDentryRequest;
using pb::metaserver::CreateDentryResponse;
using pb::metaserver::CreateInodeRequest;
using pb::metaserver::CreateInodeResponse;
using pb::metaserver::CreateManageInodeRequest;
using pb::metaserver::CreateManageInodeResponse;
using pb::metaserver::CreatePartitionRequest;
using pb::metaserver::CreatePartitionResponse;
using pb::metaserver::CreateRootInodeRequest;
using pb::metaserver::CreateRootInodeResponse;
using pb::metaserver::DeleteDentryRequest;
using pb::metaserver::DeleteDentryResponse;
using pb::metaserver::DeleteDirQuotaRequest;
using pb::metaserver::DeleteDirQuotaResponse;
using pb::metaserver::DeleteInodeRequest;
using pb::metaserver::DeleteInodeResponse;
using pb::metaserver::DeletePartitionRequest;
using pb::metaserver::DeletePartitionResponse;
using pb::metaserver::FlushDirUsagesRequest;
using pb::metaserver::FlushDirUsagesResponse;
using pb::metaserver::FlushFsUsageRequest;
using pb::metaserver::FlushFsUsageResponse;
using pb::metaserver::GetDentryRequest;
using pb::metaserver::GetDentryResponse;
using pb::metaserver::GetDirQuotaRequest;
using pb::metaserver::GetDirQuotaResponse;
using pb::metaserver::GetFsQuotaRequest;
using pb::metaserver::GetFsQuotaResponse;
using pb::metaserver::GetInodeRequest;
using pb::metaserver::GetInodeResponse;
using pb::metaserver::GetOrModifyS3ChunkInfoRequest;
using pb::metaserver::GetOrModifyS3ChunkInfoResponse;
using pb::metaserver::GetVolumeExtentRequest;
using pb::metaserver::GetVolumeExtentResponse;
using pb::metaserver::ListDentryRequest;
using pb::metaserver::ListDentryResponse;
using pb::metaserver::LoadDirQuotasRequest;
using pb::metaserver::LoadDirQuotasResponse;
using pb::metaserver::PrepareRenameTxRequest;
using pb::metaserver::PrepareRenameTxResponse;
using pb::metaserver::SetDirQuotaRequest;
using pb::metaserver::SetDirQuotaResponse;
using pb::metaserver::SetFsQuotaRequest;
using pb::metaserver::SetFsQuotaResponse;
using pb::metaserver::UpdateInodeRequest;
using pb::metaserver::UpdateInodeResponse;
using pb::metaserver::UpdateVolumeExtentRequest;
using pb::metaserver::UpdateVolumeExtentResponse;

using pb::metaserver::MetaStatusCode;
using pb::metaserver::VolumeExtentList;

using common::StreamConnection;
using storage::Iterator;
using utils::TimeUtility;

MetaOperator::~MetaOperator() {
  if (ownRequest_ && request_) {
    delete request_;
    request_ = nullptr;
  }
}

void MetaOperator::Propose() {
  brpc::ClosureGuard doneGuard(done_);

  // check if current node is leader
  if (!IsLeaderTerm()) {
    RedirectRequest();
    return;
  }

  // check if operator can bypass propose to raft
  if (CanBypassPropose()) {
    FastApplyTask();
    doneGuard.release();
    return;
  }

  // propose to raft
  if (ProposeTask()) {
    doneGuard.release();
  }
}

void MetaOperator::RedirectRequest() { Redirect(); }

bool MetaOperator::ProposeTask() {
  timerPropose.start();
  butil::IOBuf log;
  bool success = RaftLogCodec::Encode(GetOperatorType(), request_, &log);
  if (!success) {
    LOG(ERROR) << "meta request encode failed, type: "
               << OperatorTypeName(GetOperatorType())
               << ", request: " << request_->ShortDebugString();
    OnFailed(MetaStatusCode::UNKNOWN_ERROR);
    return false;
  }

  braft::Task task;
  task.data = &log;
  task.done = new MetaOperatorClosure(this);
  task.expected_term = node_->LeaderTerm();

  node_->Propose(task);

  return true;
}

void MetaOperator::FastApplyTask() {
  butil::Timer timer;
  timer.start();
  auto task =
      std::bind(&MetaOperator::OnApply, this, node_->GetAppliedIndex(),
                new MetaOperatorClosure(this), TimeUtility::GetTimeofDayUs());
  node_->GetApplyQueue()->Push(HashCode(), std::move(task));
  timer.stop();
  g_concurrent_fast_apply_wait_latency << timer.u_elapsed();
}

#define OPERATOR_CAN_BYPASS_PROPOSE(TYPE) \
  bool TYPE##Operator::CanBypassPropose() const { return false; }

#define READONLY_OPERATOR_CAN_BYPASS_PROPOSE(TYPE)                           \
  bool TYPE##Operator::CanBypassPropose() const {                            \
    auto* req = static_cast<const pb::metaserver::TYPE##Request*>(request_); \
    return req->has_appliedindex() &&                                        \
           node_->GetAppliedIndex() >= req->appliedindex();                  \
  }

OPERATOR_CAN_BYPASS_PROPOSE(SetFsQuota);
OPERATOR_CAN_BYPASS_PROPOSE(FlushFsUsage);
OPERATOR_CAN_BYPASS_PROPOSE(SetDirQuota);
OPERATOR_CAN_BYPASS_PROPOSE(DeleteDirQuota);
OPERATOR_CAN_BYPASS_PROPOSE(FlushDirUsages);

READONLY_OPERATOR_CAN_BYPASS_PROPOSE(GetFsQuota);
READONLY_OPERATOR_CAN_BYPASS_PROPOSE(GetDirQuota);
READONLY_OPERATOR_CAN_BYPASS_PROPOSE(LoadDirQuotas);

bool GetInodeOperator::CanBypassPropose() const {
  auto* req = static_cast<const GetInodeRequest*>(request_);
  return req->has_appliedindex() &&
         node_->GetAppliedIndex() >= req->appliedindex();
}

bool ListDentryOperator::CanBypassPropose() const {
  auto* req = static_cast<const ListDentryRequest*>(request_);
  return req->has_appliedindex() &&
         node_->GetAppliedIndex() >= req->appliedindex();
}

bool BatchGetInodeAttrOperator::CanBypassPropose() const {
  auto* req = static_cast<const BatchGetInodeAttrRequest*>(request_);
  return req->has_appliedindex() &&
         node_->GetAppliedIndex() >= req->appliedindex();
}

bool BatchGetXAttrOperator::CanBypassPropose() const {
  auto* req = static_cast<const BatchGetXAttrRequest*>(request_);
  return req->has_appliedindex() &&
         node_->GetAppliedIndex() >= req->appliedindex();
}

bool GetDentryOperator::CanBypassPropose() const {
  auto* req = static_cast<const GetDentryRequest*>(request_);
  return req->has_appliedindex() &&
         node_->GetAppliedIndex() >= req->appliedindex();
}

bool GetVolumeExtentOperator::CanBypassPropose() const {
  const auto* req = static_cast<const GetVolumeExtentRequest*>(request_);
  return req->has_appliedindex() &&
         node_->GetAppliedIndex() >= req->appliedindex();
}

#define OPERATOR_ON_APPLY(TYPE)                                                \
  void TYPE##Operator::OnApply(int64_t index, google::protobuf::Closure* done, \
                               uint64_t startTimeUs) {                         \
    brpc::ClosureGuard doneGuard(done);                                        \
    uint64_t timeUs = TimeUtility::GetTimeofDayUs();                           \
    node_->GetMetric()->WaitInQueueLatency(OperatorType::TYPE,                 \
                                           timeUs - startTimeUs);              \
    auto status = node_->GetMetaStore()->TYPE(                                 \
        static_cast<const TYPE##Request*>(request_),                           \
        static_cast<TYPE##Response*>(response_));                              \
    uint64_t executeTime = TimeUtility::GetTimeofDayUs() - timeUs;             \
    node_->GetMetric()->ExecuteLatency(OperatorType::TYPE, executeTime);       \
    if (status == MetaStatusCode::OK) {                                        \
      node_->UpdateAppliedIndex(index);                                        \
      static_cast<TYPE##Response*>(response_)->set_appliedindex(               \
          std::max<uint64_t>(index, node_->GetAppliedIndex()));                \
      node_->GetMetric()->OnOperatorComplete(                                  \
          OperatorType::TYPE, TimeUtility::GetTimeofDayUs() - startTimeUs,     \
          true);                                                               \
    } else {                                                                   \
      node_->GetMetric()->OnOperatorComplete(                                  \
          OperatorType::TYPE, TimeUtility::GetTimeofDayUs() - startTimeUs,     \
          false);                                                              \
    }                                                                          \
  }

OPERATOR_ON_APPLY(SetFsQuota);
OPERATOR_ON_APPLY(GetFsQuota);
OPERATOR_ON_APPLY(FlushFsUsage);
OPERATOR_ON_APPLY(SetDirQuota);
OPERATOR_ON_APPLY(GetDirQuota);
OPERATOR_ON_APPLY(DeleteDirQuota);
OPERATOR_ON_APPLY(LoadDirQuotas);
OPERATOR_ON_APPLY(FlushDirUsages);
OPERATOR_ON_APPLY(GetDentry);
OPERATOR_ON_APPLY(ListDentry);
OPERATOR_ON_APPLY(CreateDentry);
OPERATOR_ON_APPLY(DeleteDentry);
OPERATOR_ON_APPLY(GetInode);
OPERATOR_ON_APPLY(BatchGetInodeAttr);
OPERATOR_ON_APPLY(BatchGetXAttr);
OPERATOR_ON_APPLY(CreateInode);
OPERATOR_ON_APPLY(UpdateInode);
OPERATOR_ON_APPLY(DeleteInode);
OPERATOR_ON_APPLY(CreateRootInode);
OPERATOR_ON_APPLY(CreateManageInode);
OPERATOR_ON_APPLY(CreatePartition);
OPERATOR_ON_APPLY(DeletePartition);
OPERATOR_ON_APPLY(PrepareRenameTx);
OPERATOR_ON_APPLY(UpdateVolumeExtent);

#undef OPERATOR_ON_APPLY

// NOTE: now we need struct `brpc::Controller` for sending data by stream,
// so we redefine OnApply() and OnApplyFromLog() instead of using macro.
// It may not be an elegant implementation, can you provide a better idea?
void GetOrModifyS3ChunkInfoOperator::OnApply(int64_t index,
                                             google::protobuf::Closure* done,
                                             uint64_t startTimeUs) {
  MetaStatusCode rc;
  auto request = static_cast<const GetOrModifyS3ChunkInfoRequest*>(request_);
  auto response = static_cast<GetOrModifyS3ChunkInfoResponse*>(response_);
  auto metastore = node_->GetMetaStore();
  std::shared_ptr<StreamConnection> connection;
  std::shared_ptr<Iterator> iterator;
  auto streamServer = metastore->GetStreamServer();

  {
    brpc::ClosureGuard doneGuard(done);

    rc = metastore->GetOrModifyS3ChunkInfo(request, response, &iterator);
    if (rc == MetaStatusCode::OK) {
      node_->UpdateAppliedIndex(index);
      response->set_appliedindex(
          std::max<uint64_t>(index, node_->GetAppliedIndex()));
      node_->GetMetric()->OnOperatorComplete(
          OperatorType::GetOrModifyS3ChunkInfo,
          TimeUtility::GetTimeofDayUs() - startTimeUs, true);
    } else {
      node_->GetMetric()->OnOperatorComplete(
          OperatorType::GetOrModifyS3ChunkInfo,
          TimeUtility::GetTimeofDayUs() - startTimeUs, false);
    }

    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_);
    if (rc != MetaStatusCode::OK || !request->returns3chunkinfomap() ||
        !request->supportstreaming()) {
      return;
    }

    // rc == MetaStatusCode::OK && streaming
    connection = streamServer->Accept(cntl);
    if (nullptr == connection) {
      LOG(ERROR) << "Accept stream connection failed in server-side";
      response->set_statuscode(MetaStatusCode::RPC_STREAM_ERROR);
      return;
    }
  }

  rc = metastore->SendS3ChunkInfoByStream(connection, iterator);
  if (rc != MetaStatusCode::OK) {
    LOG(ERROR) << "Sending s3chunkinfo by stream failed";
  }
}

void GetVolumeExtentOperator::OnApply(int64_t index,
                                      google::protobuf::Closure* done,
                                      uint64_t startTimeUs) {
  brpc::ClosureGuard doneGuard(done);
  const auto* request = static_cast<const GetVolumeExtentRequest*>(request_);
  auto* response = static_cast<GetVolumeExtentResponse*>(response_);
  auto* metaStore = node_->GetMetaStore();

  auto st = metaStore->GetVolumeExtent(request, response);
  node_->GetMetric()->OnOperatorComplete(
      OperatorType::GetVolumeExtent,
      TimeUtility::GetTimeofDayUs() - startTimeUs, st == MetaStatusCode::OK);

  if (st != MetaStatusCode::OK) {
    return;
  }

  response->set_appliedindex(index);
  if (!request->streaming()) {
    return;
  }

  // in streaming mode, swap slices out and send them by streaming
  VolumeExtentList extents;
  response->mutable_slices()->Swap(&extents);
  response->clear_slices();

  // accept client's streaming request
  auto* cntl = static_cast<brpc::Controller*>(cntl_);
  auto streamingServer = metaStore->GetStreamServer();
  auto connection = streamingServer->Accept(cntl);
  if (connection == nullptr) {
    LOG(ERROR) << "Accept streaming connection failed";
    response->set_statuscode(MetaStatusCode::RPC_STREAM_ERROR);
    return;
  }

  // run done
  done->Run();
  doneGuard.release();

  // send volume extent
  st = StreamingSendVolumeExtent(connection.get(), extents);
  if (st != MetaStatusCode::OK) {
    LOG(ERROR) << "Send volume extents by stream failed";
  }
}

#define OPERATOR_ON_APPLY_FROM_LOG(TYPE)                                 \
  void TYPE##Operator::OnApplyFromLog(uint64_t startTimeUs) {            \
    std::unique_ptr<TYPE##Operator> selfGuard(this);                     \
    TYPE##Response response;                                             \
    auto status = node_->GetMetaStore()->TYPE(                           \
        static_cast<const TYPE##Request*>(request_), &response);         \
    node_->GetMetric()->OnOperatorCompleteFromLog(                       \
        OperatorType::TYPE, TimeUtility::GetTimeofDayUs() - startTimeUs, \
        status == MetaStatusCode::OK);                                   \
  }

OPERATOR_ON_APPLY_FROM_LOG(SetFsQuota);
OPERATOR_ON_APPLY_FROM_LOG(FlushFsUsage);
OPERATOR_ON_APPLY_FROM_LOG(SetDirQuota);
OPERATOR_ON_APPLY_FROM_LOG(DeleteDirQuota);
OPERATOR_ON_APPLY_FROM_LOG(FlushDirUsages);
OPERATOR_ON_APPLY_FROM_LOG(CreateDentry);
OPERATOR_ON_APPLY_FROM_LOG(DeleteDentry);
OPERATOR_ON_APPLY_FROM_LOG(CreateInode);
OPERATOR_ON_APPLY_FROM_LOG(UpdateInode);
OPERATOR_ON_APPLY_FROM_LOG(DeleteInode);
OPERATOR_ON_APPLY_FROM_LOG(CreateRootInode);
OPERATOR_ON_APPLY_FROM_LOG(CreateManageInode);
OPERATOR_ON_APPLY_FROM_LOG(CreatePartition);
OPERATOR_ON_APPLY_FROM_LOG(DeletePartition);
OPERATOR_ON_APPLY_FROM_LOG(PrepareRenameTx);
OPERATOR_ON_APPLY_FROM_LOG(UpdateVolumeExtent);

#undef OPERATOR_ON_APPLY_FROM_LOG

void GetOrModifyS3ChunkInfoOperator::OnApplyFromLog(uint64_t startTimeUs) {
  std::unique_ptr<GetOrModifyS3ChunkInfoOperator> selfGuard(this);
  GetOrModifyS3ChunkInfoRequest request;
  GetOrModifyS3ChunkInfoResponse response;
  std::shared_ptr<Iterator> iterator;
  request = *static_cast<const GetOrModifyS3ChunkInfoRequest*>(request_);
  request.set_returns3chunkinfomap(false);
  auto status = node_->GetMetaStore()->GetOrModifyS3ChunkInfo(
      &request, &response, &iterator);
  node_->GetMetric()->OnOperatorCompleteFromLog(
      OperatorType::GetOrModifyS3ChunkInfo,
      TimeUtility::GetTimeofDayUs() - startTimeUs,
      status == MetaStatusCode::OK);
}

#define READONLY_OPERATOR_ON_APPLY_FROM_LOG(TYPE)             \
  void TYPE##Operator::OnApplyFromLog(uint64_t startTimeUs) { \
    (void)startTimeUs;                                        \
    std::unique_ptr<TYPE##Operator> selfGuard(this);          \
  }

// below operator are readonly, so on apply from log do nothing
READONLY_OPERATOR_ON_APPLY_FROM_LOG(GetFsQuota);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(GetDirQuota);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(LoadDirQuotas);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(GetDentry);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(ListDentry);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(GetInode);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(BatchGetInodeAttr);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(BatchGetXAttr);
READONLY_OPERATOR_ON_APPLY_FROM_LOG(GetVolumeExtent);

#undef READONLY_OPERATOR_ON_APPLY_FROM_LOG

#define OPERATOR_REDIRECT(TYPE)                              \
  void TYPE##Operator::Redirect() {                          \
    static_cast<TYPE##Response*>(response_)->set_statuscode( \
        MetaStatusCode::REDIRECTED);                         \
  }

OPERATOR_REDIRECT(SetFsQuota);
OPERATOR_REDIRECT(GetFsQuota);
OPERATOR_REDIRECT(FlushFsUsage);
OPERATOR_REDIRECT(SetDirQuota);
OPERATOR_REDIRECT(GetDirQuota);
OPERATOR_REDIRECT(DeleteDirQuota);
OPERATOR_REDIRECT(LoadDirQuotas);
OPERATOR_REDIRECT(FlushDirUsages);
OPERATOR_REDIRECT(GetDentry);
OPERATOR_REDIRECT(ListDentry);
OPERATOR_REDIRECT(CreateDentry);
OPERATOR_REDIRECT(DeleteDentry);
OPERATOR_REDIRECT(GetInode);
OPERATOR_REDIRECT(BatchGetInodeAttr);
OPERATOR_REDIRECT(BatchGetXAttr);
OPERATOR_REDIRECT(CreateInode);
OPERATOR_REDIRECT(UpdateInode);
OPERATOR_REDIRECT(GetOrModifyS3ChunkInfo);
OPERATOR_REDIRECT(DeleteInode);
OPERATOR_REDIRECT(CreateRootInode);
OPERATOR_REDIRECT(CreateManageInode);
OPERATOR_REDIRECT(CreatePartition);
OPERATOR_REDIRECT(DeletePartition);
OPERATOR_REDIRECT(PrepareRenameTx);
OPERATOR_REDIRECT(GetVolumeExtent);
OPERATOR_REDIRECT(UpdateVolumeExtent);

#undef OPERATOR_REDIRECT

#define OPERATOR_ON_FAILED(TYPE)                                   \
  void TYPE##Operator::OnFailed(MetaStatusCode code) {             \
    static_cast<TYPE##Response*>(response_)->set_statuscode(code); \
  }

OPERATOR_ON_FAILED(SetFsQuota);
OPERATOR_ON_FAILED(GetFsQuota);
OPERATOR_ON_FAILED(FlushFsUsage);
OPERATOR_ON_FAILED(SetDirQuota);
OPERATOR_ON_FAILED(GetDirQuota);
OPERATOR_ON_FAILED(DeleteDirQuota);
OPERATOR_ON_FAILED(LoadDirQuotas);
OPERATOR_ON_FAILED(FlushDirUsages);
OPERATOR_ON_FAILED(GetDentry);
OPERATOR_ON_FAILED(ListDentry);
OPERATOR_ON_FAILED(CreateDentry);
OPERATOR_ON_FAILED(DeleteDentry);
OPERATOR_ON_FAILED(GetInode);
OPERATOR_ON_FAILED(BatchGetInodeAttr);
OPERATOR_ON_FAILED(BatchGetXAttr);
OPERATOR_ON_FAILED(CreateInode);
OPERATOR_ON_FAILED(UpdateInode);
OPERATOR_ON_FAILED(GetOrModifyS3ChunkInfo);
OPERATOR_ON_FAILED(DeleteInode);
OPERATOR_ON_FAILED(CreateRootInode);
OPERATOR_ON_FAILED(CreateManageInode);
OPERATOR_ON_FAILED(CreatePartition);
OPERATOR_ON_FAILED(DeletePartition);
OPERATOR_ON_FAILED(PrepareRenameTx);
OPERATOR_ON_FAILED(GetVolumeExtent);
OPERATOR_ON_FAILED(UpdateVolumeExtent);

#undef OPERATOR_ON_FAILED

#define OPERATOR_HASH_CODE(TYPE)                                       \
  uint64_t TYPE##Operator::HashCode() const {                          \
    return static_cast<const TYPE##Request*>(request_)->partitionid(); \
  }

OPERATOR_HASH_CODE(GetDentry);
OPERATOR_HASH_CODE(ListDentry);
OPERATOR_HASH_CODE(CreateDentry);
OPERATOR_HASH_CODE(DeleteDentry);
OPERATOR_HASH_CODE(GetInode);
OPERATOR_HASH_CODE(BatchGetInodeAttr);
OPERATOR_HASH_CODE(BatchGetXAttr);
OPERATOR_HASH_CODE(CreateInode);
OPERATOR_HASH_CODE(UpdateInode);
OPERATOR_HASH_CODE(GetOrModifyS3ChunkInfo);
OPERATOR_HASH_CODE(DeleteInode);
OPERATOR_HASH_CODE(CreateRootInode);
OPERATOR_HASH_CODE(CreateManageInode);
OPERATOR_HASH_CODE(PrepareRenameTx);
OPERATOR_HASH_CODE(DeletePartition);
OPERATOR_HASH_CODE(GetVolumeExtent);
OPERATOR_HASH_CODE(UpdateVolumeExtent);

#undef OPERATOR_HASH_CODE

#define SUPER_PARTITION_OPERATOR_HASH_CODE(TYPE)                \
  uint64_t TYPE##Operator::HashCode() const {                   \
    return static_cast<const TYPE##Request*>(request_)->fsid(); \
  }

SUPER_PARTITION_OPERATOR_HASH_CODE(SetFsQuota);
SUPER_PARTITION_OPERATOR_HASH_CODE(GetFsQuota);
SUPER_PARTITION_OPERATOR_HASH_CODE(FlushFsUsage);
SUPER_PARTITION_OPERATOR_HASH_CODE(SetDirQuota);
SUPER_PARTITION_OPERATOR_HASH_CODE(GetDirQuota);
SUPER_PARTITION_OPERATOR_HASH_CODE(DeleteDirQuota);
SUPER_PARTITION_OPERATOR_HASH_CODE(LoadDirQuotas);
SUPER_PARTITION_OPERATOR_HASH_CODE(FlushDirUsages);

#undef SUPER_PARTITION_OPERATOR_HASH_CODE

#define PARTITION_OPERATOR_HASH_CODE(TYPE)             \
  uint64_t TYPE##Operator::HashCode() const {          \
    return static_cast<const TYPE##Request*>(request_) \
        ->partition()                                  \
        .partitionid();                                \
  }

PARTITION_OPERATOR_HASH_CODE(CreatePartition);

#undef PARTITION_OPERATOR_HASH_CODE

#define OPERATOR_TYPE(TYPE)                              \
  OperatorType TYPE##Operator::GetOperatorType() const { \
    return OperatorType::TYPE;                           \
  }

OPERATOR_TYPE(SetFsQuota);
OPERATOR_TYPE(GetFsQuota);
OPERATOR_TYPE(FlushFsUsage);
OPERATOR_TYPE(SetDirQuota);
OPERATOR_TYPE(GetDirQuota);
OPERATOR_TYPE(DeleteDirQuota);
OPERATOR_TYPE(LoadDirQuotas);
OPERATOR_TYPE(FlushDirUsages);
OPERATOR_TYPE(GetDentry);
OPERATOR_TYPE(ListDentry);
OPERATOR_TYPE(CreateDentry);
OPERATOR_TYPE(DeleteDentry);
OPERATOR_TYPE(GetInode);
OPERATOR_TYPE(BatchGetInodeAttr);
OPERATOR_TYPE(BatchGetXAttr);
OPERATOR_TYPE(CreateInode);
OPERATOR_TYPE(UpdateInode);
OPERATOR_TYPE(GetOrModifyS3ChunkInfo);
OPERATOR_TYPE(DeleteInode);
OPERATOR_TYPE(CreateRootInode);
OPERATOR_TYPE(CreateManageInode);
OPERATOR_TYPE(PrepareRenameTx);
OPERATOR_TYPE(CreatePartition);
OPERATOR_TYPE(DeletePartition);
OPERATOR_TYPE(GetVolumeExtent);
OPERATOR_TYPE(UpdateVolumeExtent);

#undef OPERATOR_TYPE

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs
