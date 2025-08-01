// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/service/mds_service.h"

#include <fmt/format.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "brpc/controller.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/server.h"
#include "mdsv2/service/service_helper.h"
#include "mdsv2/statistics/fs_stat.h"

namespace dingofs {
namespace mdsv2 {

const std::string kReadWorkerSetName = "READ_WORKER_SET";
const std::string kWriteWorkerSetName = "WRITE_WORKER_SET";

template <typename T>
static Status ValidateRequest(T* request, FileSystemSPtr file_system) {
  if (request->context().epoch() < file_system->Epoch()) {
    return Status(pb::error::EROUTER_EPOCH_CHANGE,
                  fmt::format("epoch change, {}<{}", request->context().epoch(), file_system->Epoch()));

  } else if (request->context().epoch() > file_system->Epoch()) {
    return file_system->RefreshFsInfo();
  }

  return Status::OK();
}

MDSServiceImpl::MDSServiceImpl(const MetaServiceOption& option, FileSystemSetSPtr file_system_set,
                               GcProcessorSPtr gc_processor, FsStatsUPtr fs_stat)
    : option_(option), file_system_set_(file_system_set), gc_processor_(gc_processor), fs_stat_(std::move(fs_stat)) {}

bool MDSServiceImpl::Init() {
  read_worker_set_ =
      SimpleWorkerSet::NewUnique(kReadWorkerSetName, option_.read_worker_num(), option_.read_worker_max_pending_num(),
                                 option_.read_worker_use_pthread(), false);
  if (!read_worker_set_->Init()) {
    DINGO_LOG(ERROR) << "init service read worker set fail!";
    return false;
  }

  write_worker_set_ =
      SimpleWorkerSet::NewUnique(kWriteWorkerSetName, option_.write_worker_num(),
                                 option_.write_worker_max_pending_num(), option_.write_worker_use_pthread(), false);
  if (!write_worker_set_->Init()) {
    DINGO_LOG(ERROR) << "init service write worker set fail!";
    return false;
  }

  return true;
}

void MDSServiceImpl::Destroy() {
  read_worker_set_->Destroy();
  write_worker_set_->Destroy();
}

FileSystemSPtr MDSServiceImpl::GetFileSystem(uint32_t fs_id) { return file_system_set_->GetFileSystem(fs_id); }

void MDSServiceImpl::DoHeartbeat(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::HeartbeatRequest* request, pb::mdsv2::HeartbeatResponse* response,
                                 TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto heartbeat = Server::GetInstance().GetHeartbeat();
  if (BAIDU_UNLIKELY(heartbeat == nullptr)) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "heartbeat is nullptr");
  }

  Status status;
  if (request->role() == pb::mdsv2::ROLE_MDS) {
    auto mds = request->mds();
    status = heartbeat->SendHeartbeat(mds);

  } else if (request->role() == pb::mdsv2::ROLE_CLIENT) {
    auto client = request->client();
    status = heartbeat->SendHeartbeat(client);

  } else {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER, "role is illegal");
  }

  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Heartbeat(google::protobuf::RpcController* controller, const pb::mdsv2::HeartbeatRequest* request,
                               pb::mdsv2::HeartbeatResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoHeartbeat(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetMDSList(google::protobuf::RpcController* controller, const pb::mdsv2::GetMDSListRequest*,
                                  pb::mdsv2::GetMDSListResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto heartbeat = Server::GetInstance().GetHeartbeat();
  if (BAIDU_UNLIKELY(heartbeat == nullptr)) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EINTERNAL, "heartbeat is nullptr");
  }

  std::vector<MdsEntry> mdses;
  auto status = heartbeat->GetMDSList(mdses);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Helper::VectorToPbRepeated(mdses, response->mutable_mdses());
}

void MDSServiceImpl::GetMDSList(google::protobuf::RpcController* controller,
                                const pb::mdsv2::GetMDSListRequest* request, pb::mdsv2::GetMDSListResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetMDSList(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                                pb::mdsv2::CreateFsResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto& mds_meta = Server::GetInstance().GetMDSMeta();

  FileSystemSet::CreateFsParam param;
  param.mds_id = mds_meta.ID();
  param.fs_name = request->fs_name();
  param.chunk_size = request->chunk_size();
  param.block_size = request->block_size();
  param.fs_type = request->fs_type();
  param.fs_extra = request->fs_extra();
  param.enable_sum_in_dir = request->enable_sum_in_dir();
  param.owner = request->owner();
  param.capacity = request->capacity();
  param.recycle_time_hour = request->recycle_time_hour();
  param.partition_type = request->partition_type();

  pb::mdsv2::FsInfo fs_info;
  auto status = file_system_set_->CreateFs(param, fs_info);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  *response->mutable_fs_info() = fs_info;
}

// fs interface
void MDSServiceImpl::CreateFs(google::protobuf::RpcController* controller, const pb::mdsv2::CreateFsRequest* request,
                              pb::mdsv2::CreateFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }

    if (request->chunk_size() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "chunk size is zero");
    }

    if (request->block_size() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "block size is zero");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoCreateFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
                               pb::mdsv2::MountFsResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  Context ctx;
  auto status = file_system_set_->MountFs(ctx, request->fs_name(), request->mount_point());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::MountFs(google::protobuf::RpcController* controller, const pb::mdsv2::MountFsRequest* request,
                             pb::mdsv2::MountFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }
    const auto& mount_point = request->mount_point();
    if (mount_point.client_id().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "client_id is empty");
    }
    if (mount_point.hostname().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "hostname is empty");
    }
    if (mount_point.path().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "mount point path is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoMountFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoUmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                                pb::mdsv2::UmountFsResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  Context ctx;
  auto status = file_system_set_->UmountFs(ctx, request->fs_name(), request->client_id());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UmountFs(google::protobuf::RpcController* controller, const pb::mdsv2::UmountFsRequest* request,
                              pb::mdsv2::UmountFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }

    if (request->client_id().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "client_id is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUmountFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoDeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                                pb::mdsv2::DeleteFsResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  Context ctx;
  auto status = file_system_set_->DeleteFs(ctx, request->fs_name(), request->is_force());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DeleteFs(google::protobuf::RpcController* controller, const pb::mdsv2::DeleteFsRequest* request,
                              pb::mdsv2::DeleteFsResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoDeleteFs(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsInfo(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::GetFsInfoRequest* request, pb::mdsv2::GetFsInfoResponse* response,
                                 TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  std::string fs_name = request->fs_name();
  if (request->fs_id() > 0) {
    auto file_system = GetFileSystem(request->fs_id());
    if (file_system == nullptr) {
      return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
    }

    fs_name = file_system->FsName();
  }

  Context ctx;
  pb::mdsv2::FsInfo fs_info;
  auto status = file_system_set_->GetFsInfo(ctx, fs_name, fs_info);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_fs_info()->Swap(&fs_info);
}

void MDSServiceImpl::GetFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::GetFsInfoRequest* request,
                               pb::mdsv2::GetFsInfoResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_name().empty() && request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_name or fs_id is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetFsInfo(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoListFsInfo(google::protobuf::RpcController* controller, const pb::mdsv2::ListFsInfoRequest*,
                                  pb::mdsv2::ListFsInfoResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  Context ctx;
  std::vector<pb::mdsv2::FsInfo> fs_infoes;
  auto status = file_system_set_->GetAllFsInfo(ctx, fs_infoes);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Helper::VectorToPbRepeated(fs_infoes, response->mutable_fs_infos());
}

void MDSServiceImpl::ListFsInfo(google::protobuf::RpcController* controller,
                                const pb::mdsv2::ListFsInfoRequest* request, pb::mdsv2::ListFsInfoResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListFsInfo(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoUpdateFsInfo(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::UpdateFsInfoRequest* request,
                                    pb::mdsv2::UpdateFsInfoResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  if (request->fs_name().empty()) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER, "fs name is empty");
  }

  Context ctx;
  auto status = file_system_set_->UpdateFsInfo(ctx, request->fs_name(), request->fs_info());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UpdateFsInfo(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::UpdateFsInfoRequest* request,
                                  pb::mdsv2::UpdateFsInfoResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUpdateFsInfo(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetDentry(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::GetDentryRequest* request, pb::mdsv2::GetDentryResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  Dentry dentry;
  auto status = file_system->GetDentry(ctx, request->parent(), request->name(), dentry);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_dentry()->CopyFrom(dentry.Copy());
}

// dentry interface
void MDSServiceImpl::GetDentry(google::protobuf::RpcController* controller, const pb::mdsv2::GetDentryRequest* request,
                               pb::mdsv2::GetDentryResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetDentry(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoListDentry(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::ListDentryRequest* request, pb::mdsv2::ListDentryResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<Dentry> dentries;
  uint32_t limit = request->limit() > 0 ? request->limit() : UINT32_MAX;
  auto status =
      file_system->ListDentry(ctx, request->parent(), request->last(), limit, request->is_only_dir(), dentries);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& dentry : dentries) {
    *response->add_dentries() = dentry.Copy();
  }
}

void MDSServiceImpl::ListDentry(google::protobuf::RpcController* controller,
                                const pb::mdsv2::ListDentryRequest* request, pb::mdsv2::ListDentryResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->parent() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "parent is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListDentry(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetInode(google::protobuf::RpcController* controller, const pb::mdsv2::GetInodeRequest* request,
                                pb::mdsv2::GetInodeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  auto status = file_system->GetInode(ctx, request->ino(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

// inode interface
void MDSServiceImpl::GetInode(google::protobuf::RpcController* controller, const pb::mdsv2::GetInodeRequest* request,
                              pb::mdsv2::GetInodeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetInode(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoBatchGetInode(google::protobuf::RpcController* controller,
                                     const pb::mdsv2::BatchGetInodeRequest* request,
                                     pb::mdsv2::BatchGetInodeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<EntryOut> entries;
  auto status = file_system->BatchGetInode(ctx, Helper::PbRepeatedToVector(request->inoes()), entries);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& entry : entries) {
    response->add_inodes()->Swap(&entry.attr);
  }
}

void MDSServiceImpl::BatchGetInode(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::BatchGetInodeRequest* request,
                                   pb::mdsv2::BatchGetInodeResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoBatchGetInode(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoBatchGetXAttr(google::protobuf::RpcController* controller,
                                     const pb::mdsv2::BatchGetXAttrRequest* request,
                                     pb::mdsv2::BatchGetXAttrResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<pb::mdsv2::XAttr> xattrs;
  auto status = file_system->BatchGetXAttr(ctx, Helper::PbRepeatedToVector(request->inoes()), xattrs);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& xattr : xattrs) {
    response->add_xattrs()->CopyFrom(xattr);
  }
}

void MDSServiceImpl::BatchGetXAttr(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::BatchGetXAttrRequest* request,
                                   pb::mdsv2::BatchGetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoBatchGetXAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

// high level interface
void MDSServiceImpl::DoLookup(google::protobuf::RpcController* controller, const pb::mdsv2::LookupRequest* request,
                              pb::mdsv2::LookupResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->Lookup(ctx, request->parent(), request->name(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::Lookup(google::protobuf::RpcController* controller, const pb::mdsv2::LookupRequest* request,
                            pb::mdsv2::LookupResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLookup(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
                             pb::mdsv2::MkNodResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  FileSystem::MkNodParam param;
  param.parent = request->parent();
  param.name = request->name();
  param.mode = request->mode();
  param.uid = request->uid();
  param.gid = request->gid();
  param.rdev = request->rdev();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->MkNod(ctx, param, entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::MkNod(google::protobuf::RpcController* controller, const pb::mdsv2::MkNodRequest* request,
                           pb::mdsv2::MkNodResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoMkNod(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoMkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
                             pb::mdsv2::MkDirResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  FileSystem::MkDirParam param;
  param.parent = request->parent();
  param.name = request->name();
  param.mode = request->mode();
  param.uid = request->uid();
  param.gid = request->gid();
  param.rdev = request->rdev();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->MkDir(ctx, param, entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::MkDir(google::protobuf::RpcController* controller, const pb::mdsv2::MkDirRequest* request,
                           pb::mdsv2::MkDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoMkDir(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
                             pb::mdsv2::RmDirResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  status = file_system->RmDir(ctx, request->parent(), request->name());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::RmDir(google::protobuf::RpcController* controller, const pb::mdsv2::RmDirRequest* request,
                           pb::mdsv2::RmDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRmDir(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoReadDir(google::protobuf::RpcController* controller, const pb::mdsv2::ReadDirRequest* request,
                               pb::mdsv2::ReadDirResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<EntryOut> entry_outs;
  status = file_system->ReadDir(ctx, request->ino(), request->last_name(), request->limit(), request->with_attr(),
                                entry_outs);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& entry_out : entry_outs) {
    auto* mut_entry = response->add_entries();
    mut_entry->set_name(entry_out.name);
    mut_entry->set_ino(entry_out.attr.ino());
    if (request->with_attr()) {
      mut_entry->mutable_inode()->Swap(&entry_out.attr);
    }
  }
}

void MDSServiceImpl::ReadDir(google::protobuf::RpcController* controller, const pb::mdsv2::ReadDirRequest* request,
                             pb::mdsv2::ReadDirResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadDir(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoOpen(google::protobuf::RpcController* controller, const pb::mdsv2::OpenRequest* request,
                            pb::mdsv2::OpenResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version(), req_ctx.client_id());

  std::string session_id;
  uint64_t version;
  status = file_system->Open(ctx, request->ino(), request->flags(), session_id, version);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_session_id(session_id);
  response->set_version(version);
}

void MDSServiceImpl::Open(google::protobuf::RpcController* controller, const pb::mdsv2::OpenRequest* request,
                          pb::mdsv2::OpenResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoOpen(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRelease(google::protobuf::RpcController* controller, const pb::mdsv2::ReleaseRequest* request,
                               pb::mdsv2::ReleaseResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version(), req_ctx.client_id());

  status = file_system->Release(ctx, request->ino(), request->session_id());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Release(google::protobuf::RpcController* controller, const pb::mdsv2::ReleaseRequest* request,
                             pb::mdsv2::ReleaseResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRelease(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoLink(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
                            pb::mdsv2::LinkResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->Link(ctx, request->ino(), request->new_parent(), request->new_name(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::Link(google::protobuf::RpcController* controller, const pb::mdsv2::LinkRequest* request,
                          pb::mdsv2::LinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoUnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
                              pb::mdsv2::UnLinkResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  status = file_system->UnLink(ctx, request->parent(), request->name());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::UnLink(google::protobuf::RpcController* controller, const pb::mdsv2::UnLinkRequest* request,
                            pb::mdsv2::UnLinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoUnLink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSymlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
                               pb::mdsv2::SymlinkResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  ctx.SetAncestors(Helper::PbRepeatedToVector(request->context().ancestors()));

  EntryOut entry_out;
  status = file_system->Symlink(ctx, request->symlink(), request->new_parent(), request->new_name(), request->uid(),
                                request->gid(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (!status.ok()) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
  response->set_parent_version(entry_out.parent_version);
}

void MDSServiceImpl::Symlink(google::protobuf::RpcController* controller, const pb::mdsv2::SymlinkRequest* request,
                             pb::mdsv2::SymlinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSymlink(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                                pb::mdsv2::ReadLinkResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::string symlink;
  status = file_system->ReadLink(ctx, request->ino(), symlink);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_symlink(symlink);
}

void MDSServiceImpl::ReadLink(google::protobuf::RpcController* controller, const pb::mdsv2::ReadLinkRequest* request,
                              pb::mdsv2::ReadLinkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadLink(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetAttrRequest* request,
                               pb::mdsv2::GetAttrResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->GetAttr(ctx, request->ino(), entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::GetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetAttrRequest* request,
                             pb::mdsv2::GetAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetAttrRequest* request,
                               pb::mdsv2::SetAttrResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  FileSystem::SetAttrParam param;
  auto& attr = param.attr;
  attr.set_fs_id(request->fs_id());
  attr.set_ino(request->ino());
  attr.set_length(request->length());
  attr.set_ctime(request->ctime());
  attr.set_mtime(request->mtime());
  attr.set_atime(request->atime());
  attr.set_uid(request->uid());
  attr.set_gid(request->gid());
  attr.set_mode(request->mode());
  param.to_set = request->to_set();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry_out;
  status = file_system->SetAttr(ctx, request->ino(), param, entry_out);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_inode()->Swap(&entry_out.attr);
}

void MDSServiceImpl::SetAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetAttrRequest* request,
                             pb::mdsv2::SetAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetXAttrRequest* request,
                                pb::mdsv2::GetXAttrResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::string value;
  status = file_system->GetXAttr(ctx, request->ino(), request->name(), value);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_value(value);
}

void MDSServiceImpl::GetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::GetXAttrRequest* request,
                              pb::mdsv2::GetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }
    if (request->name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetXAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetXAttrRequest* request,
                                pb::mdsv2::SetXAttrResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  uint64_t version;
  status = file_system->SetXAttr(ctx, request->ino(), request->xattrs(), version);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_inode_version(version);
}

void MDSServiceImpl::SetXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::SetXAttrRequest* request,
                              pb::mdsv2::SetXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }
    if (request->xattrs().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "xattrs is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetXAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRemoveXAttr(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::RemoveXAttrRequest* request,
                                   pb::mdsv2::RemoveXAttrResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  uint64_t version;
  status = file_system->RemoveXAttr(ctx, request->ino(), request->name(), version);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_inode_version(version);
}

void MDSServiceImpl::RemoveXAttr(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::RemoveXAttrRequest* request, pb::mdsv2::RemoveXAttrResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is empty");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is empty");
    }
    if (request->name().empty()) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "name is empty");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRemoveXAttr(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoListXAttr(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::ListXAttrRequest* request, pb::mdsv2::ListXAttrResponse* response,
                                 TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  Inode::XAttrMap xattrs;
  status = file_system->GetXAttr(ctx, request->ino(), xattrs);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_xattrs()->swap(xattrs);
}

void MDSServiceImpl::ListXAttr(google::protobuf::RpcController* controller, const pb::mdsv2::ListXAttrRequest* request,
                               pb::mdsv2::ListXAttrResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoListXAttr(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoRename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
                              pb::mdsv2::RenameResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  FileSystem::RenameParam param;
  param.old_parent = request->old_parent();
  param.old_name = request->old_name();
  param.new_parent = request->new_parent();
  param.new_name = request->new_name();
  param.old_ancestors = Helper::PbRepeatedToVector(request->old_ancestors());
  param.new_ancestors = Helper::PbRepeatedToVector(request->new_ancestors());

  uint64_t old_parent_version, new_parent_version;
  auto status = file_system->CommitRename(ctx, param, old_parent_version, new_parent_version);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_old_parent_version(old_parent_version);
  response->set_new_parent_version(new_parent_version);
}

void MDSServiceImpl::Rename(google::protobuf::RpcController* controller, const pb::mdsv2::RenameRequest* request,
                            pb::mdsv2::RenameResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoRename(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoAllocSliceId(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::AllocSliceIdRequest* request,
                                    pb::mdsv2::AllocSliceIdResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  if (request->alloc_num() == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::EILLEGAL_PARAMTETER,
                                   "param alloc_num is error");
  }

  uint64_t slice_id;
  auto status = file_system_set_->AllocSliceId(request->alloc_num(), request->min_slice_id(), slice_id);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->set_slice_id(slice_id);
}

void MDSServiceImpl::AllocSliceId(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::AllocSliceIdRequest* request,
                                  pb::mdsv2::AllocSliceIdResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoAllocSliceId(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoWriteSlice(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::WriteSliceRequest* request, pb::mdsv2::WriteSliceResponse* response,
                                  TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  status = file_system->WriteSlice(ctx, request->parent(), request->ino(), request->chunk_index(),
                                   Helper::PbRepeatedToVector(request->slices()));
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
}

void MDSServiceImpl::WriteSlice(google::protobuf::RpcController* controller,
                                const pb::mdsv2::WriteSliceRequest* request, pb::mdsv2::WriteSliceResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->parent() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "parent is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }
    if (request->chunk_index() < 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "chunk_index is negative");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoWriteSlice(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoReadSlice(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::ReadSliceRequest* request, pb::mdsv2::ReadSliceResponse* response,
                                 TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<pb::mdsv2::Slice> slices;
  status = file_system->ReadSlice(ctx, request->ino(), request->chunk_index(), slices);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  Helper::VectorToPbRepeated(slices, response->mutable_slices());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
}

void MDSServiceImpl::ReadSlice(google::protobuf::RpcController* controller, const pb::mdsv2::ReadSliceRequest* request,
                               pb::mdsv2::ReadSliceResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }
    if (request->chunk_index() < 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "chunk_index is negative");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoReadSlice(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoFallocate(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::FallocateRequest* request, pb::mdsv2::FallocateResponse* response,
                                 TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  auto status = ValidateRequest(request, file_system);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  EntryOut entry;
  status = file_system->Fallocate(ctx, request->ino(), request->mode(), request->offset(), request->len(), entry);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::Fallocate(google::protobuf::RpcController* controller, const pb::mdsv2::FallocateRequest* request,
                               pb::mdsv2::FallocateResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }
    if (request->len() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "len is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    brpc::ClosureGuard done_guard(svr_done);
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoFallocate(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCompactChunk(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::CompactChunkRequest* request,
                                    pb::mdsv2::CompactChunkResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::vector<pb::mdsv2::TrashSlice> trash_slices;
  status = file_system->CompactChunk(ctx, request->ino(), request->chunk_index(), trash_slices);
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());

  for (auto& slice : trash_slices) {
    response->add_trash_slices()->Swap(&slice);
  }
}

void MDSServiceImpl::CompactChunk(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::CompactChunkRequest* request,
                                  pb::mdsv2::CompactChunkResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoCompactChunk(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCleanTrashSlice(google::protobuf::RpcController* controller,
                                       const pb::mdsv2::CleanTrashSliceRequest* request,
                                       pb::mdsv2::CleanTrashSliceResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  auto& trace = ctx.GetTrace();

  status = gc_processor_->ManualCleanDelSlice(trace, request->fs_id(), request->ino(), request->chunk_index());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::CleanTrashSlice(google::protobuf::RpcController* controller,
                                     const pb::mdsv2::CleanTrashSliceRequest* request,
                                     pb::mdsv2::CleanTrashSliceResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoCleanTrashSlice(controller, request, response, svr_done);
  });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoCleanDelFile(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::CleanDelFileRequest* request,
                                    pb::mdsv2::CleanDelFileResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  // validate request
  auto validate_fn = [&]() -> Status {
    if (request->fs_id() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "fs_id is 0");
    }

    if (request->ino() == 0) {
      return Status(pb::error::EILLEGAL_PARAMTETER, "ino is 0");
    }

    return Status::OK();
  };

  auto status = validate_fn();
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());
  auto& trace = ctx.GetTrace();

  status = gc_processor_->ManualCleanDelFile(trace, request->fs_id(), request->ino());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::CleanDelFile(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::CleanDelFileRequest* request,
                                  pb::mdsv2::CleanDelFileResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoCleanDelFile(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetFsQuota(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::SetFsQuotaRequest* request, pb::mdsv2::SetFsQuotaResponse* response,
                                  TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }
  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  auto status = quota_manager.SetFsQuota(ctx.GetTrace(), request->quota());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

// quota interface
void MDSServiceImpl::SetFsQuota(google::protobuf::RpcController* controller,
                                const pb::mdsv2::SetFsQuotaRequest* request, pb::mdsv2::SetFsQuotaResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetFsQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsQuota(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::GetFsQuotaRequest* request, pb::mdsv2::GetFsQuotaResponse* response,
                                  TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }
  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  pb::mdsv2::Quota quota;
  auto status = quota_manager.GetFsQuota(ctx.GetTrace(), quota);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_quota()->Swap(&quota);
}

void MDSServiceImpl::GetFsQuota(google::protobuf::RpcController* controller,
                                const pb::mdsv2::GetFsQuotaRequest* request, pb::mdsv2::GetFsQuotaResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetFsQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoFlushFsUsage(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::FlushFsUsageRequest* request,
                                    pb::mdsv2::FlushFsUsageResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  //   auto file_system = GetFileSystem(request->fs_id());
  // if (file_system == nullptr) {
  //   return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  // }
  // auto& quota_manager = file_system->GetQuotaManager();

  // const auto& req_ctx = request->context();
  // Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  // auto status = quota_manager->FlushFsUsage(ctx, request->fs_id(), request->usage());
  // ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  // if (BAIDU_UNLIKELY(!status.ok())) {
  //   return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  // }
}

void MDSServiceImpl::FlushFsUsage(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::FlushFsUsageRequest* request,
                                  pb::mdsv2::FlushFsUsageResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoFlushFsUsage(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetDirQuota(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::SetDirQuotaRequest* request,
                                   pb::mdsv2::SetDirQuotaResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }
  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  auto status = quota_manager.SetDirQuota(ctx.GetTrace(), request->ino(), request->quota());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::SetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::SetDirQuotaRequest* request, pb::mdsv2::SetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetDirQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetDirQuota(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::GetDirQuotaRequest* request,
                                   pb::mdsv2::GetDirQuotaResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }
  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  pb::mdsv2::Quota quota;
  auto status = quota_manager.GetDirQuota(ctx.GetTrace(), request->ino(), quota);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_quota()->Swap(&quota);
}

void MDSServiceImpl::GetDirQuota(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::GetDirQuotaRequest* request, pb::mdsv2::GetDirQuotaResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetDirQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoDeleteDirQuota(google::protobuf::RpcController* controller,
                                      const pb::mdsv2::DeleteDirQuotaRequest* request,
                                      pb::mdsv2::DeleteDirQuotaResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }
  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  auto status = quota_manager.DeleteDirQuota(ctx.GetTrace(), request->ino());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::DeleteDirQuota(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::DeleteDirQuotaRequest* request,
                                    pb::mdsv2::DeleteDirQuotaResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoDeleteDirQuota(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoLoadDirQuotas(google::protobuf::RpcController* controller,
                                     const pb::mdsv2::LoadDirQuotasRequest* request,
                                     pb::mdsv2::LoadDirQuotasResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  auto file_system = GetFileSystem(request->fs_id());
  if (file_system == nullptr) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }
  auto& quota_manager = file_system->GetQuotaManager();

  const auto& req_ctx = request->context();
  Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  std::map<uint64_t, pb::mdsv2::Quota> quotas;
  auto status = quota_manager.LoadDirQuotas(ctx.GetTrace(), quotas);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (const auto& [ino, quota] : quotas) {
    response->mutable_quotas()->insert(std::make_pair(ino, quota));
  }
}

void MDSServiceImpl::LoadDirQuotas(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::LoadDirQuotasRequest* request,
                                   pb::mdsv2::LoadDirQuotasResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoLoadDirQuotas(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoFlushDirUsages(google::protobuf::RpcController* controller,
                                      const pb::mdsv2::FlushDirUsagesRequest* request,
                                      pb::mdsv2::FlushDirUsagesResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  // const auto& req_ctx = request->context();
  // Context ctx(req_ctx.is_bypass_cache(), req_ctx.inode_version());

  // std::map<uint64_t, pb::mdsv2::Usage> usages;
  // for (const auto& [ino, usage] : request->usages()) {
  //   usages[ino] = usage;
  // }

  // auto status = quota_processor_->FlushDirUsages(ctx, request->fs_id(), usages);
  // ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  // if (BAIDU_UNLIKELY(!status.ok())) {
  //   return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  // }
}

void MDSServiceImpl::FlushDirUsages(google::protobuf::RpcController* controller,
                                    const pb::mdsv2::FlushDirUsagesRequest* request,
                                    pb::mdsv2::FlushDirUsagesResponse* response, google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoFlushDirUsages(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoSetFsStats(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::SetFsStatsRequest* request, pb::mdsv2::SetFsStatsResponse* response,
                                  TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  uint32_t fs_id = file_system_set_->GetFsId(request->fs_name());
  if (fs_id == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx;
  auto status = fs_stat_->UploadFsStat(ctx, fs_id, request->stats());
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }
}

void MDSServiceImpl::SetFsStats(google::protobuf::RpcController* controller,
                                const pb::mdsv2::SetFsStatsRequest* request, pb::mdsv2::SetFsStatsResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoSetFsStats(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsStats(google::protobuf::RpcController* controller,
                                  const pb::mdsv2::GetFsStatsRequest* request, pb::mdsv2::GetFsStatsResponse* response,
                                  TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  uint32_t fs_id = file_system_set_->GetFsId(request->fs_name());
  if (fs_id == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx;
  pb::mdsv2::FsStatsData stats;
  auto status = fs_stat_->GetFsStat(ctx, fs_id, stats);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  response->mutable_stats()->Swap(&stats);
}

void MDSServiceImpl::GetFsStats(google::protobuf::RpcController* controller,
                                const pb::mdsv2::GetFsStatsRequest* request, pb::mdsv2::GetFsStatsResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoGetFsStats(controller, request, response, svr_done); });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::DoGetFsPerSecondStats(google::protobuf::RpcController* controller,
                                           const pb::mdsv2::GetFsPerSecondStatsRequest* request,
                                           pb::mdsv2::GetFsPerSecondStatsResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  uint32_t fs_id = file_system_set_->GetFsId(request->fs_name());
  if (fs_id == 0) {
    return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
  }

  Context ctx;
  std::map<uint64_t, pb::mdsv2::FsStatsData> stats;
  auto status = fs_stat_->GetFsStatsPerSecond(ctx, fs_id, stats);
  ServiceHelper::SetResponseInfo(ctx.GetTrace(), response->mutable_info());
  if (BAIDU_UNLIKELY(!status.ok())) {
    return ServiceHelper::SetError(response->mutable_error(), status.error_code(), status.error_str());
  }

  for (auto& [time, stat] : stats) {
    response->mutable_stats()->insert(std::make_pair(time, std::move(stat)));
  }
}

void MDSServiceImpl::GetFsPerSecondStats(google::protobuf::RpcController* controller,
                                         const pb::mdsv2::GetFsPerSecondStatsRequest* request,
                                         pb::mdsv2::GetFsPerSecondStatsResponse* response,
                                         google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>([this, controller, request, response, svr_done]() {
    DoGetFsPerSecondStats(controller, request, response, svr_done);
  });

  bool ret = read_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

void MDSServiceImpl::CheckAlive(google::protobuf::RpcController* controller,
                                const pb::mdsv2::CheckAliveRequest* request, pb::mdsv2::CheckAliveResponse* response,
                                google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(svr_done);
}

void MDSServiceImpl::DoNotifyBuddy(google::protobuf::RpcController* controller,
                                   const pb::mdsv2::NotifyBuddyRequest* request,
                                   pb::mdsv2::NotifyBuddyResponse* response, TraceClosure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  done->SetQueueWaitTime();

  for (const auto& message : request->messages()) {
    switch (message.type()) {
      case pb::mdsv2::NotifyBuddyRequest::TYPE_REFRESH_FS_INFO: {
        auto status = file_system_set_->RefreshFsInfo(message.refresh_fs_info().fs_name());
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format("refresh fs info fail, status({})", status.error_str());
        }

      } break;

      case pb::mdsv2::NotifyBuddyRequest::TYPE_REFRESH_INODE: {
        auto file_system = GetFileSystem(message.fs_id());
        if (file_system == nullptr) {
          return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
        }

        auto& mut_message = const_cast<pb::mdsv2::NotifyBuddyRequest::Message&>(message);
        file_system->RefreshInode(*mut_message.mutable_refresh_inode()->mutable_inode());
      } break;

      case pb::mdsv2::NotifyBuddyRequest::TYPE_CLEAN_PARTITION_CACHE: {
        auto file_system = GetFileSystem(message.fs_id());
        if (file_system == nullptr) {
          return ServiceHelper::SetError(response->mutable_error(), pb::error::ENOT_FOUND, "fs not found");
        }

        file_system->GetPartitionCache().Delete(message.clean_partition_cache().ino());

      } break;

      default:
        DINGO_LOG(FATAL) << "unknown message type: " << message.type();
    }
  }
}

void MDSServiceImpl::NotifyBuddy(google::protobuf::RpcController* controller,
                                 const pb::mdsv2::NotifyBuddyRequest* request, pb::mdsv2::NotifyBuddyResponse* response,
                                 google::protobuf::Closure* done) {
  auto* svr_done = new ServiceClosure(__func__, done, request, response);

  // Run in queue.
  auto task = std::make_shared<ServiceTask>(
      [this, controller, request, response, svr_done]() { DoNotifyBuddy(controller, request, response, svr_done); });

  bool ret = write_worker_set_->Execute(task);
  if (BAIDU_UNLIKELY(!ret)) {
    brpc::ClosureGuard done_guard(svr_done);
    ServiceHelper::SetError(response->mutable_error(), pb::error::EREQUEST_FULL,
                            "WorkerSet queue is full, please wait and retry");
  }
}

}  // namespace mdsv2

}  // namespace dingofs