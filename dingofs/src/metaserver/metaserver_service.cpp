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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "dingofs/src/metaserver/metaserver_service.h"

#include <list>
#include <string>

#include "dingofs/src/metaserver/copyset/meta_operator.h"
#include "dingofs/src/metaserver/metaservice_closure.h"

static bvar::LatencyRecorder g_oprequest_in_service_before_propose_latency(
    "oprequest_in_service_before_propose");

namespace dingofs {
namespace metaserver {

using ::dingofs::metaserver::copyset::BatchGetInodeAttrOperator;
using ::dingofs::metaserver::copyset::BatchGetXAttrOperator;
using ::dingofs::metaserver::copyset::CreateDentryOperator;
using ::dingofs::metaserver::copyset::CreateInodeOperator;
using ::dingofs::metaserver::copyset::CreateManageInodeOperator;
using ::dingofs::metaserver::copyset::CreatePartitionOperator;
using ::dingofs::metaserver::copyset::CreateRootInodeOperator;
using ::dingofs::metaserver::copyset::DeleteDentryOperator;
using ::dingofs::metaserver::copyset::DeleteDirQuotaOperator;
using ::dingofs::metaserver::copyset::DeleteInodeOperator;
using ::dingofs::metaserver::copyset::DeletePartitionOperator;
using ::dingofs::metaserver::copyset::FlushDirUsagesOperator;
using ::dingofs::metaserver::copyset::FlushFsUsageOperator;
using ::dingofs::metaserver::copyset::GetDentryOperator;
using ::dingofs::metaserver::copyset::GetDirQuotaOperator;
using ::dingofs::metaserver::copyset::GetFsQuotaOperator;
using ::dingofs::metaserver::copyset::GetInodeOperator;
using ::dingofs::metaserver::copyset::GetOrModifyS3ChunkInfoOperator;
using ::dingofs::metaserver::copyset::GetVolumeExtentOperator;
using ::dingofs::metaserver::copyset::ListDentryOperator;
using ::dingofs::metaserver::copyset::LoadDirQuotasOperator;
using ::dingofs::metaserver::copyset::PrepareRenameTxOperator;
using ::dingofs::metaserver::copyset::SetDirQuotaOperator;
using ::dingofs::metaserver::copyset::SetFsQuotaOperator;
using ::dingofs::metaserver::copyset::UpdateInodeOperator;
using ::dingofs::metaserver::copyset::UpdateInodeS3VersionOperator;
using ::dingofs::metaserver::copyset::UpdateVolumeExtentOperator;

namespace {

struct OperatorHelper {
  OperatorHelper(CopysetNodeManager* manager, InflightThrottle* throttle)
      : manager(manager), throttle(throttle) {}

  template <typename OperatorT, typename RequestT, typename ResponseT>
  void operator()(google::protobuf::RpcController* cntl,
                  const RequestT* request, ResponseT* response,
                  google::protobuf::Closure* done, PoolId poolId,
                  CopysetId copysetId) {
    butil::Timer timer;
    timer.start();
    // check if overloaded
    brpc::ClosureGuard doneGuard(done);
    if (throttle->IsOverLoad()) {
      LOG_EVERY_N(WARNING, 100)
          << "service overload, request: " << request->ShortDebugString();
      response->set_statuscode(MetaStatusCode::OVERLOAD);
      return;
    }

    auto node = manager->GetCopysetNode(poolId, copysetId);

    if (!node) {
      LOG(WARNING) << "Copyset not found, request: "
                   << request->ShortDebugString();
      response->set_statuscode(MetaStatusCode::COPYSET_NOTEXIST);
      return;
    }

    auto* op =
        new OperatorT(node, cntl, request, response,
                      new MetaServiceClosure(throttle, doneGuard.release()));
    timer.stop();
    g_oprequest_in_service_before_propose_latency << timer.u_elapsed();
    node->GetMetric()->NewArrival(op->GetOperatorType());
    op->Propose();
  }

  CopysetNodeManager* manager;
  InflightThrottle* throttle;
};

}  // namespace

#define DEFINE_RPC_METHOD(method)                                            \
  void MetaServerServiceImpl::method(                                        \
      ::google::protobuf::RpcController* controller,                         \
      const ::dingofs::metaserver::method##Request* request,                 \
      ::dingofs::metaserver::method##Response* response,                     \
      ::google::protobuf::Closure* done) {                                   \
    OperatorHelper helper(copysetNodeManager_, inflightThrottle_);           \
    helper.operator()<method##Operator>(controller, request, response, done, \
                                        request->poolid(),                   \
                                        request->copysetid());               \
  }

DEFINE_RPC_METHOD(SetFsQuota);
DEFINE_RPC_METHOD(GetFsQuota);
DEFINE_RPC_METHOD(FlushFsUsage);
DEFINE_RPC_METHOD(SetDirQuota);
DEFINE_RPC_METHOD(GetDirQuota);
DEFINE_RPC_METHOD(DeleteDirQuota);
DEFINE_RPC_METHOD(LoadDirQuotas);
DEFINE_RPC_METHOD(FlushDirUsages);

void MetaServerServiceImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::GetDentryRequest* request,
    ::dingofs::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetDentryOperator>(controller, request, response, done,
                                       request->poolid(), request->copysetid());
}

void MetaServerServiceImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::ListDentryRequest* request,
    ::dingofs::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);

  helper.operator()<ListDentryOperator>(controller, request, response, done,
                                        request->poolid(),
                                        request->copysetid());
}

void MetaServerServiceImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateDentryRequest* request,
    ::dingofs::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateDentryOperator>(controller, request, response, done,
                                          request->poolid(),
                                          request->copysetid());
}

void MetaServerServiceImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::DeleteDentryRequest* request,
    ::dingofs::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<DeleteDentryOperator>(controller, request, response, done,
                                          request->poolid(),
                                          request->copysetid());
}

void MetaServerServiceImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::GetInodeRequest* request,
    ::dingofs::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetInodeOperator>(controller, request, response, done,
                                      request->poolid(), request->copysetid());
}

void MetaServerServiceImpl::BatchGetInodeAttr(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::BatchGetInodeAttrRequest* request,
    ::dingofs::metaserver::BatchGetInodeAttrResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<BatchGetInodeAttrOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::BatchGetXAttr(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::BatchGetXAttrRequest* request,
    ::dingofs::metaserver::BatchGetXAttrResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<BatchGetXAttrOperator>(controller, request, response, done,
                                           request->poolid(),
                                           request->copysetid());
}

void MetaServerServiceImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateInodeRequest* request,
    ::dingofs::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateInodeOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateRootInodeRequest* request,
    ::dingofs::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateRootInodeOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::CreateManageInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateManageInodeRequest* request,
    ::dingofs::metaserver::CreateManageInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreateManageInodeOperator>(controller, request, response,
                                               done, request->poolid(),
                                               request->copysetid());
}

void MetaServerServiceImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::UpdateInodeRequest* request,
    ::dingofs::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<UpdateInodeOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::GetOrModifyS3ChunkInfo(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::GetOrModifyS3ChunkInfoRequest* request,
    ::dingofs::metaserver::GetOrModifyS3ChunkInfoResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetOrModifyS3ChunkInfoOperator>(
      controller, request, response, done, request->poolid(),
      request->copysetid());
}

void MetaServerServiceImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::DeleteInodeRequest* request,
    ::dingofs::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<DeleteInodeOperator>(controller, request, response, done,
                                         request->poolid(),
                                         request->copysetid());
}

void MetaServerServiceImpl::CreatePartition(
    google::protobuf::RpcController* controller,
    const CreatePartitionRequest* request, CreatePartitionResponse* response,
    google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<CreatePartitionOperator>(
      controller, request, response, done, request->partition().poolid(),
      request->partition().copysetid());
}

void MetaServerServiceImpl::DeletePartition(
    google::protobuf::RpcController* controller,
    const DeletePartitionRequest* request, DeletePartitionResponse* response,
    google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<DeletePartitionOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::PrepareRenameTx(
    google::protobuf::RpcController* controller,
    const PrepareRenameTxRequest* request, PrepareRenameTxResponse* response,
    google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<PrepareRenameTxOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::GetVolumeExtent(
    ::google::protobuf::RpcController* controller,
    const GetVolumeExtentRequest* request, GetVolumeExtentResponse* response,
    ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<GetVolumeExtentOperator>(controller, request, response,
                                             done, request->poolid(),
                                             request->copysetid());
}

void MetaServerServiceImpl::UpdateVolumeExtent(
    ::google::protobuf::RpcController* controller,
    const UpdateVolumeExtentRequest* request,
    UpdateVolumeExtentResponse* response, ::google::protobuf::Closure* done) {
  OperatorHelper helper(copysetNodeManager_, inflightThrottle_);
  helper.operator()<UpdateVolumeExtentOperator>(controller, request, response,
                                                done, request->poolid(),
                                                request->copysetid());
}

}  // namespace metaserver
}  // namespace dingofs
