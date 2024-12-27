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
 * @Project: dingo
 * @Date: 2021-06-24 16:26:02
 * @Author: chenwei
 */

#ifndef DINGOFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
#define DINGOFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
#include <gmock/gmock.h>

#include "dingofs/proto/copyset.pb.h"
#include "dingofs/proto/metaserver.pb.h"

namespace dingofs {
namespace metaserver {
class MockMetaserverService : public MetaServerService {
 public:
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
  MOCK_METHOD4(GetInode,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::GetInodeRequest* request,
                    ::dingofs::metaserver::GetInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(CreateInode,
               void(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::CreateInodeRequest* request,
                    ::dingofs::metaserver::CreateInodeResponse* response,
                    ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      CreateRootInode,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::CreateRootInodeRequest* request,
           ::dingofs::metaserver::CreateRootInodeResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      CreateManageInode,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::CreateManageInodeRequest* request,
           ::dingofs::metaserver::CreateManageInodeResponse* response,
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
      CreatePartition,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::CreatePartitionRequest* request,
           ::dingofs::metaserver::CreatePartitionResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      DeletePartition,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::metaserver::DeletePartitionRequest* request,
           ::dingofs::metaserver::DeletePartitionResponse* response,
           ::google::protobuf::Closure* done));
};

namespace copyset {
class MockCopysetService : public CopysetService {
 public:
  MOCK_METHOD4(CreateCopysetNode,
               void(::google::protobuf::RpcController* controller,
                    const CreateCopysetRequest* request,
                    CreateCopysetResponse* response,
                    ::google::protobuf::Closure* done));
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_TEST_MDS_MOCK_MOCK_METASERVER_H_
