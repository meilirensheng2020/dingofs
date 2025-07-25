/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_HEARTBEAT_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_HEARTBEAT_H_

#include <memory>

#include "cache/cachegroup/cache_group_node_member.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/executor/executor.h"

namespace dingofs {
namespace cache {

class CacheGroupNodeHeartbeat {
 public:
  virtual ~CacheGroupNodeHeartbeat() = default;

  virtual void Start() = 0;
  virtual void Shutdown() = 0;
};

using CacheGroupNodeHeartbeatUPtr = std::unique_ptr<CacheGroupNodeHeartbeat>;

class CacheGroupNodeHeartbeatImpl final : public CacheGroupNodeHeartbeat {
 public:
  using MdsClientSPtr = std::shared_ptr<stub::rpcclient::MdsClient>;

  CacheGroupNodeHeartbeatImpl(CacheGroupNodeMemberSPtr member,
                              MdsClientSPtr mds_client);

  void Start() override;
  void Shutdown() override;

 private:
  void SendHeartbeat();

  int64_t GetCacheHitCount();
  int64_t GetCacheMissCount();

 private:
  std::atomic<bool> running_;
  CacheGroupNodeMemberSPtr member_;
  MdsClientSPtr mds_client_;
  std::unique_ptr<Executor> executor_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_HEARTBEAT_H_
