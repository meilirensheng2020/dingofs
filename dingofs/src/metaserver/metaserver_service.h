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

#ifndef DINGOFS_SRC_METASERVER_METASERVER_SERVICE_H_
#define DINGOFS_SRC_METASERVER_METASERVER_SERVICE_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include <memory>

#include "dingofs/proto/metaserver.pb.h"
#include "dingofs/src/metaserver/copyset/copyset_node_manager.h"
#include "dingofs/src/metaserver/inflight_throttle.h"

namespace dingofs {
namespace metaserver {

using ::dingofs::metaserver::copyset::CopysetNodeManager;

#define DECLARE_RPC_METHOD(method)                                   \
  void method(::google::protobuf::RpcController* controller,         \
              const ::dingofs::metaserver::method##Request* request, \
              ::dingofs::metaserver::method##Response* response,     \
              ::google::protobuf::Closure* done) override

class MetaServerServiceImpl : public MetaServerService {
 public:
  MetaServerServiceImpl(CopysetNodeManager* copysetNodeManager,
                        InflightThrottle* inflightThrottle)
      : copysetNodeManager_(copysetNodeManager),
        inflightThrottle_(inflightThrottle) {}

  DECLARE_RPC_METHOD(SetFsQuota);
  DECLARE_RPC_METHOD(GetFsQuota);
  DECLARE_RPC_METHOD(FlushFsUsage);
  DECLARE_RPC_METHOD(SetDirQuota);
  DECLARE_RPC_METHOD(GetDirQuota);
  DECLARE_RPC_METHOD(DeleteDirQuota);
  DECLARE_RPC_METHOD(LoadDirQuotas);
  DECLARE_RPC_METHOD(FlushDirUsages);

  void GetDentry(::google::protobuf::RpcController* controller,
                 const ::dingofs::metaserver::GetDentryRequest* request,
                 ::dingofs::metaserver::GetDentryResponse* response,
                 ::google::protobuf::Closure* done) override;
  void ListDentry(::google::protobuf::RpcController* controller,
                  const ::dingofs::metaserver::ListDentryRequest* request,
                  ::dingofs::metaserver::ListDentryResponse* response,
                  ::google::protobuf::Closure* done) override;
  void CreateDentry(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::CreateDentryRequest* request,
                    ::dingofs::metaserver::CreateDentryResponse* response,
                    ::google::protobuf::Closure* done) override;
  void DeleteDentry(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::DeleteDentryRequest* request,
                    ::dingofs::metaserver::DeleteDentryResponse* response,
                    ::google::protobuf::Closure* done) override;
  void GetInode(::google::protobuf::RpcController* controller,
                const ::dingofs::metaserver::GetInodeRequest* request,
                ::dingofs::metaserver::GetInodeResponse* response,
                ::google::protobuf::Closure* done) override;
  void BatchGetInodeAttr(
      ::google::protobuf::RpcController* controller,
      const ::dingofs::metaserver::BatchGetInodeAttrRequest* request,
      ::dingofs::metaserver::BatchGetInodeAttrResponse* response,
      ::google::protobuf::Closure* done) override;
  void BatchGetXAttr(::google::protobuf::RpcController* controller,
                     const ::dingofs::metaserver::BatchGetXAttrRequest* request,
                     ::dingofs::metaserver::BatchGetXAttrResponse* response,
                     ::google::protobuf::Closure* done) override;
  void CreateInode(::google::protobuf::RpcController* controller,
                   const ::dingofs::metaserver::CreateInodeRequest* request,
                   ::dingofs::metaserver::CreateInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
  void CreateRootInode(
      ::google::protobuf::RpcController* controller,
      const ::dingofs::metaserver::CreateRootInodeRequest* request,
      ::dingofs::metaserver::CreateRootInodeResponse* response,
      ::google::protobuf::Closure* done) override;
  void CreateManageInode(
      ::google::protobuf::RpcController* controller,
      const ::dingofs::metaserver::CreateManageInodeRequest* request,
      ::dingofs::metaserver::CreateManageInodeResponse* response,
      ::google::protobuf::Closure* done) override;
  void UpdateInode(::google::protobuf::RpcController* controller,
                   const ::dingofs::metaserver::UpdateInodeRequest* request,
                   ::dingofs::metaserver::UpdateInodeResponse* response,
                   ::google::protobuf::Closure* done) override;
  void GetOrModifyS3ChunkInfo(
      ::google::protobuf::RpcController* controller,
      const ::dingofs::metaserver::GetOrModifyS3ChunkInfoRequest* request,
      ::dingofs::metaserver::GetOrModifyS3ChunkInfoResponse* response,
      ::google::protobuf::Closure* done) override;
  void DeleteInode(::google::protobuf::RpcController* controller,
                   const ::dingofs::metaserver::DeleteInodeRequest* request,
                   ::dingofs::metaserver::DeleteInodeResponse* response,
                   ::google::protobuf::Closure* done) override;

  void CreatePartition(google::protobuf::RpcController* controller,
                       const CreatePartitionRequest* request,
                       CreatePartitionResponse* response,
                       google::protobuf::Closure* done) override;

  void DeletePartition(google::protobuf::RpcController* controller,
                       const DeletePartitionRequest* request,
                       DeletePartitionResponse* response,
                       google::protobuf::Closure* done) override;

  void PrepareRenameTx(google::protobuf::RpcController* controller,
                       const PrepareRenameTxRequest* request,
                       PrepareRenameTxResponse* response,
                       google::protobuf::Closure* done) override;

  void GetVolumeExtent(::google::protobuf::RpcController* controller,
                       const GetVolumeExtentRequest* request,
                       GetVolumeExtentResponse* response,
                       ::google::protobuf::Closure* done) override;

  void UpdateVolumeExtent(::google::protobuf::RpcController* controller,
                          const UpdateVolumeExtentRequest* request,
                          UpdateVolumeExtentResponse* response,
                          ::google::protobuf::Closure* done) override;

 private:
  CopysetNodeManager* copysetNodeManager_;
  InflightThrottle* inflightThrottle_;
};
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_METASERVER_SERVICE_H_
