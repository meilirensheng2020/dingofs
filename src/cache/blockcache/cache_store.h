/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-19
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_
#define DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_

#include <glog/logging.h>

#include <functional>
#include <string>

#include "base/string/string.h"
#include "cache/blockcache/block_reader.h"
#include "cache/common/common.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using dingofs::base::string::StrFormat;
using dingofs::base::string::Strs2Ints;
using dingofs::base::string::StrSplit;

struct BlockKey {
  BlockKey() : fs_id(0), ino(0), id(0), index(0), version(0) {}

  BlockKey(uint64_t fs_id, uint64_t ino, uint64_t id, uint64_t index,
           uint64_t version)
      : fs_id(fs_id), ino(ino), id(id), index(index), version(version) {}

  BlockKey(pb::cache::blockcache::BlockKey pb)
      : fs_id(pb.fs_id()),
        ino(pb.ino()),
        id(pb.id()),
        index(pb.index()),
        version(pb.version()) {}

  std::string Filename() const {
    return StrFormat("%llu_%llu_%llu_%llu_%llu", fs_id, ino, id, index,
                     version);
  }

  std::string StoreKey() const {
    return StrFormat("blocks/%d/%d/%s", id / 1000 / 1000, id / 1000,
                     Filename());
  }

  pb::cache::blockcache::BlockKey ToPb() const {
    pb::cache::blockcache::BlockKey pb;
    pb.set_fs_id(fs_id);
    pb.set_ino(ino);
    pb.set_id(id);
    pb.set_index(index);
    pb.set_version(version);
    return pb;
  }

  bool ParseFilename(const std::string_view& filename) {
    auto strs = StrSplit(filename, "_");
    return Strs2Ints(strs, {&fs_id, &ino, &id, &index, &version});
  }

  uint64_t fs_id;    // filesystem id
  uint64_t ino;      // inode id
  uint64_t id;       // chunkid
  uint64_t index;    // block index (offset/chunkSize)
  uint64_t version;  // compaction version
};

struct Block {
  Block(const char* data, size_t size) : data(data), size(size) {}

  const char* data;
  size_t size;
};

enum class BlockFrom : uint8_t {
  kCtoFlush = 0,
  kNoctoFlush = 1,
  kReload = 2,
  kUnknown = 3,
};

struct BlockContext {
  BlockContext() : from(BlockFrom::kUnknown) {}

  BlockContext(BlockFrom from) : from(from) {}

  BlockContext(BlockFrom from, const std::string& store_id)
      : from(from), store_id(store_id) {
    if (!store_id.empty()) {  // Only for block which from reload
      CHECK(from == BlockFrom::kReload);
    }
  }

  BlockFrom from;
  std::string store_id;
};

class CacheStore {
 public:
  using UploadFunc = std::function<void(
      const BlockKey& key, const std::string& stage_path, BlockContext ctx)>;

 public:
  virtual ~CacheStore() = default;

  virtual Status Init(UploadFunc uploader) = 0;

  virtual Status Shutdown() = 0;

  virtual Status Stage(const BlockKey& key, const Block& block,
                       BlockContext ctx) = 0;

  virtual Status RemoveStage(const BlockKey& key, BlockContext ctx) = 0;

  virtual Status Cache(const BlockKey& key, const Block& block) = 0;

  virtual Status Load(const BlockKey& key,
                      std::shared_ptr<BlockReader>& reader) = 0;

  virtual bool IsCached(const BlockKey& key) = 0;

  virtual std::string Id() = 0;
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BLOCKCACHE_CACHE_STORE_H_
