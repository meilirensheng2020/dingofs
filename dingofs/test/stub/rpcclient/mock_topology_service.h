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
 * @Date: 2021-09-14 16:54:35
 * @Author: chenwei
 */

#ifndef DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_
#define DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_

#include <gmock/gmock.h>

#include "dingofs/proto/topology.pb.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
class MockTopologyService : public dingofs::mds::topology::TopologyService {
 public:
  MockTopologyService() : TopologyService() {}
  ~MockTopologyService() = default;

  MOCK_METHOD4(
      GetMetaServer,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::mds::topology::GetMetaServerInfoRequest* request,
           ::dingofs::mds::topology::GetMetaServerInfoResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      GetMetaServerListInCopysets,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::mds::topology::GetMetaServerListInCopySetsRequest*
               request,
           ::dingofs::mds::topology::GetMetaServerListInCopySetsResponse*
               response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      CreatePartition,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::mds::topology::CreatePartitionRequest* request,
           ::dingofs::mds::topology::CreatePartitionResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      ListPartition,
      void(::google::protobuf::RpcController* controller,
           const ::dingofs::mds::topology::ListPartitionRequest* request,
           ::dingofs::mds::topology::ListPartitionResponse* response,
           ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      GetCopysetOfPartition,
      void(
          ::google::protobuf::RpcController* controller,
          const ::dingofs::mds::topology::GetCopysetOfPartitionRequest* request,
          ::dingofs::mds::topology::GetCopysetOfPartitionResponse* response,
          ::google::protobuf::Closure* done));
  MOCK_METHOD4(
      AllocOrGetMemcacheCluster,
      void(
          ::google::protobuf::RpcController* controller,
          const ::dingofs::mds::topology::AllocOrGetMemcacheClusterRequest*
              request,
          ::dingofs::mds::topology::AllocOrGetMemcacheClusterResponse* response,
          ::google::protobuf::Closure* done));
};
}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_RPCCLIENT_MOCK_TOPOLOGY_SERVICE_H_
