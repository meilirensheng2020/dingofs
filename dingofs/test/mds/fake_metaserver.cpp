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
 * @Date: 2021-06-24 16:55:17
 * @Author: chenwei
 */
#include "dingofs/test/mds/fake_metaserver.h"

namespace dingofs {
namespace metaserver {
void FakeMetaserverImpl::GetDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::GetDentryRequest* request,
    ::dingofs::metaserver::GetDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::ListDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::ListDentryRequest* request,
    ::dingofs::metaserver::ListDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::CreateDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateDentryRequest* request,
    ::dingofs::metaserver::CreateDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::DeleteDentry(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::DeleteDentryRequest* request,
    ::dingofs::metaserver::DeleteDentryResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::GetInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::GetInodeRequest* request,
    ::dingofs::metaserver::GetInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::CreateInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateInodeRequest* request,
    ::dingofs::metaserver::CreateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::CreateRootInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::CreateRootInodeRequest* request,
    ::dingofs::metaserver::CreateRootInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::UpdateInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::UpdateInodeRequest* request,
    ::dingofs::metaserver::UpdateInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}

void FakeMetaserverImpl::DeleteInode(
    ::google::protobuf::RpcController* controller,
    const ::dingofs::metaserver::DeleteInodeRequest* request,
    ::dingofs::metaserver::DeleteInodeResponse* response,
    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard doneGuard(done);
  // brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
  MetaStatusCode status = MetaStatusCode::OK;
  response->set_statuscode(status);
  return;
}
}  // namespace metaserver
}  // namespace dingofs
