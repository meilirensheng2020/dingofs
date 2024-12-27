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
 * Created Date: Thur Jun 16 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/proto/metaserver.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockMetaServerService : public dingofs::metaserver::MetaServerService {
 public:
  MockMetaServerService() : MetaServerService() {}
  ~MockMetaServerService() = default;

  MOCK_METHOD4(GetDentry,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::GetDentryRequest* request,
                    ::dingofs::metaserver::GetDentryResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(ListDentry,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::ListDentryRequest* request,
                    ::dingofs::metaserver::ListDentryResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreateDentry,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::CreateDentryRequest* request,
                    ::dingofs::metaserver::CreateDentryResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(DeleteDentry,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::DeleteDentryRequest* request,
                    ::dingofs::metaserver::DeleteDentryResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      PrepareRenameTx,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::PrepareRenameTxRequest* request,
           ::dingofs::metaserver::PrepareRenameTxResponse* response,
           ::google::protobuf::Closure* done));

  MOCK_METHOD4(GetInode,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::GetInodeRequest* request,
                    ::dingofs::metaserver::GetInodeResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      BatchGetInodeAttr,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::BatchGetInodeAttrRequest* request,
           ::dingofs::metaserver::BatchGetInodeAttrResponse* response,
           ::google::protobuf::Closure* done));

  MOCK_METHOD4(BatchGetXAttr,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::BatchGetXAttrRequest* request,
                    ::dingofs::metaserver::BatchGetXAttrResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(CreateInode,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::CreateInodeRequest* request,
                    ::dingofs::metaserver::CreateInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(UpdateInode,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::UpdateInodeRequest* request,
                    ::dingofs::metaserver::UpdateInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(DeleteInode,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::DeleteInodeRequest* request,
                    ::dingofs::metaserver::DeleteInodeResponse* response,
                    ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      GetOrModifyS3ChunkInfo,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::GetOrModifyS3ChunkInfoRequest* request,
           ::dingofs::metaserver::GetOrModifyS3ChunkInfoResponse* response,
           ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      GetVolumeExtent,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::GetVolumeExtentRequest* request,
           ::dingofs::metaserver::GetVolumeExtentResponse* response,
           ::google::protobuf::Closure* done));

  MOCK_METHOD4(
      UpdateVolumeExtent,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::UpdateVolumeExtentRequest* request,
           ::dingofs::metaserver::UpdateVolumeExtentResponse* response,
           ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_METASERVER_SERVICE_H_
