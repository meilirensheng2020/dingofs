/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-03-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_FILESYSTEM_DIR_CACHE_H_
#define CURVEFS_SRC_CLIENT_FILESYSTEM_DIR_CACHE_H_

#include <list>
#include <map>
#include <memory>
#include <vector>

#include "absl/container/btree_map.h"
#include "curvefs/src/base/queue/message_queue.h"
#include "curvefs/src/base/time/time.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "curvefs/src/client/filesystem/metric.h"
#include "curvefs/src/utils/concurrent/concurrent.h"
#include "curvefs/src/utils/lru_cache.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curvefs::utils::LRUCache;
using ::curvefs::utils::ReadLockGuard;
using ::curvefs::utils::RWLock;
using ::curvefs::utils::WriteLockGuard;
using ::curvefs::base::queue::MessageQueue;
using ::curvefs::base::time::TimeSpec;
using ::curvefs::client::common::DirCacheOption;

class DirEntryList {
 public:
  using IterateHandler = std::function<void(DirEntry* dirEntry)>;

 public:
  DirEntryList();

  size_t Size();

  void Add(const DirEntry& dirEntry);

  void Iterate(IterateHandler handler);

  bool Get(Ino ino, DirEntry* dirEntry);

  bool UpdateAttr(Ino ino, const InodeAttr& attr);

  bool UpdateLength(Ino ino, const InodeAttr& open);

  void Clear();

  void SetMtime(TimeSpec mtime);

  TimeSpec GetMtime();

 private:
  RWLock rwlock_;
  TimeSpec mtime_;
  std::vector<DirEntry> entries_;
  absl::btree_map<Ino, uint32_t> index_;
};

class DirCache {
 public:
  using LRUType = LRUCache<Ino, std::shared_ptr<DirEntryList>>;
  using MessageType = std::shared_ptr<DirEntryList>;
  using MessageQueueType = MessageQueue<MessageType>;

 public:
  explicit DirCache(DirCacheOption option);

  void Start();

  void Stop();

  void Put(Ino parent, std::shared_ptr<DirEntryList> entries);

  bool Get(Ino parent, std::shared_ptr<DirEntryList>* entries);

  void Drop(Ino parent);

 private:
  void Delete(Ino parent, std::shared_ptr<DirEntryList> entries, bool evit);

  void Evit(size_t size);

 private:
  RWLock rwlock_;
  size_t nentries_;
  DirCacheOption option_;
  std::shared_ptr<LRUType> lru_;
  std::shared_ptr<MessageQueueType> mq_;
  std::shared_ptr<DirCacheMetric> metric_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_FILESYSTEM_DIR_CACHE_H_
