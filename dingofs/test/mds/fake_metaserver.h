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
 * @Date: 2021-06-24 16:53:31
 * @Author: chenwei
 */
#ifndef DINGOFS_TEST_MDS_FAKE_METASERVER_H_
#define DINGOFS_TEST_MDS_FAKE_METASERVER_H_

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "dingofs/proto/metaserver.pb.h"

namespace dingofs {
namespace metaserver {
class FakeMetaserverImpl : public MetaServerService {
 public:
  FakeMetaserverImpl() {}
  void GetDentry(::google::protobuf::RpcController* controller,
                 const ::dingofs::metaserver::GetDentryRequest* request,
                 ::dingofs::metaserver::GetDentryResponse* response,
                 ::google::protobuf::Closure* done);
  void ListDentry(::google::protobuf::RpcController* controller,
                  const ::dingofs::metaserver::ListDentryRequest* request,
                  ::dingofs::metaserver::ListDentryResponse* response,
                  ::google::protobuf::Closure* done);
  void CreateDentry(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::CreateDentryRequest* request,
                    ::dingofs::metaserver::CreateDentryResponse* response,
                    ::google::protobuf::Closure* done);
  void DeleteDentry(::google::protobuf::RpcController* controller,
                    const ::dingofs::metaserver::DeleteDentryRequest* request,
                    ::dingofs::metaserver::DeleteDentryResponse* response,
                    ::google::protobuf::Closure* done);
  void GetInode(::google::protobuf::RpcController* controller,
                const ::dingofs::metaserver::GetInodeRequest* request,
                ::dingofs::metaserver::GetInodeResponse* response,
                ::google::protobuf::Closure* done);
  void CreateInode(::google::protobuf::RpcController* controller,
                   const ::dingofs::metaserver::CreateInodeRequest* request,
                   ::dingofs::metaserver::CreateInodeResponse* response,
                   ::google::protobuf::Closure* done);
  void CreateRootInode(
      ::google::protobuf::RpcController* controller,
      const ::dingofs::metaserver::CreateRootInodeRequest* request,
      ::dingofs::metaserver::CreateRootInodeResponse* response,
      ::google::protobuf::Closure* done);
  void UpdateInode(::google::protobuf::RpcController* controller,
                   const ::dingofs::metaserver::UpdateInodeRequest* request,
                   ::dingofs::metaserver::UpdateInodeResponse* response,
                   ::google::protobuf::Closure* done);
  void DeleteInode(::google::protobuf::RpcController* controller,
                   const ::dingofs::metaserver::DeleteInodeRequest* request,
                   ::dingofs::metaserver::DeleteInodeResponse* response,
                   ::google::protobuf::Closure* done);
};

}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_TEST_MDS_FAKE_METASERVER_H_
