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

#include "client/vfs/data/reader/chunk_reader.h"

#include <cstdint>
#include <functional>
#include <vector>

#include "cache/utils/context.h"
#include "client/common/utils.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/data/reader/reader_common.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

ChunkReader::ChunkReader(VFSHub* hub, uint64_t ino, uint64_t index)
    : hub_(hub),
      ino_(ino),
      index_(index),
      fs_id_(hub->GetFsInfo().id),
      chunk_size_(hub->GetFsInfo().chunk_size),
      block_size_(hub->GetFsInfo().block_size),
      page_size_(hub->GetPageSize()),
      chunk_start_(index * chunk_size_),
      chunk_end_(chunk_start_ + chunk_size_) {}

void ChunkReader::BlockReadCallback(ChunkReader* reader,
                                    const BlockCacheReadReq& req,
                                    ReaderSharedState& shared, Status s) {
  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "{} ChunkReader fail read block_key: {}, buf_pos: {}, block_req: {} "
        "status: {}",
        reader->UUID(), req.key.StoreKey(), Char2Addr(req.buf_pos),
        req.block_req.ToString(), s.ToString());
  } else {
    VLOG(6) << fmt::format(
        "{} ChunkReader success read block_key: {}, buf_pos: {}, block_req: "
        "{}, io_buf_size: {}",
        reader->UUID(), req.key.StoreKey(), Char2Addr(req.buf_pos),
        req.block_req.ToString(), req.io_buffer.Size());
  }

  {
    std::lock_guard<std::mutex> lock(shared.mtx);

    if (s.ok()) {
      req.io_buffer.CopyTo(req.buf_pos);
    } else {
      if (shared.status.ok()) {
        shared.status = s;
      }
    }

    if (++shared.num_done >= shared.total) {
      shared.cv.notify_all();
    }
  }
}

void ChunkReader::ReadAsync(const ChunkReadReq& req, StatusCallback cb) {
  hub_->GetReadExecutor()->Execute([this, &req, cb]() { DoRead(req, cb); });
}

void ChunkReader::DoRead(const ChunkReadReq& req, StatusCallback cb) {
  uint64_t chunk_offset = req.offset;
  uint64_t size = req.to_read_size;
  char* buf = req.buf;

  uint64_t read_file_offset = chunk_start_ + chunk_offset;
  uint64_t end_read_file_offset = read_file_offset + size;

  uint64_t end_read_chunk_offet = chunk_offset + size;

  VLOG(4) << fmt::format(
      "{} ChunkReader Read Start buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      UUID(), Char2Addr(buf), size, chunk_offset, end_read_chunk_offet,
      read_file_offset, end_read_file_offset);

  CHECK_GE(chunk_end_, end_read_file_offset);

  uint64_t block_offset = chunk_offset % block_size_;
  uint64_t block_index = chunk_offset / block_size_;

  uint64_t remain_len = size;

  std::vector<Slice> slices;
  Status s = hub_->GetMetaSystem()->ReadSlice(ino_, index_, &slices);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("{} Read slice failed, status: {}", UUID(),
                                s.ToString());
    cb(s);
  }

  FileRange range{.offset = read_file_offset, .len = size};
  std::vector<SliceReadReq> slice_reqs = ProcessReadRequest(slices, range);

  std::vector<BlockReadReq> block_reqs;

  for (auto& slice_req : slice_reqs) {
    VLOG(6) << "{} Read slice_req: " << slice_req.ToString();

    if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, fs_id_, ino_, chunk_size_, block_size_);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    } else {
      char* buf_pos = buf + (slice_req.file_offset - read_file_offset);
      VLOG(6) << fmt::format("{} Read buf: {}, zero fill, read_size: {}",
                             UUID(), Char2Addr(buf_pos), slice_req.len);
      memset(buf_pos, 0, slice_req.len);
    }
  }

  std::vector<BlockCacheReadReq> block_cache_reqs;
  block_cache_reqs.reserve(block_reqs.size());

  for (auto& block_req : block_reqs) {
    cache::BlockKey key(fs_id_, ino_, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);

    char* buf_pos = buf + (block_req.block.file_offset +
                           block_req.block_offset - read_file_offset);

    VLOG(6) << fmt::format("{} Read block_key: {}, buf: {}, block_req: {}",
                           UUID(), key.StoreKey(), Char2Addr(buf_pos),
                           block_req.ToString());

    cache::RangeOption option;
    option.retrive = true;
    option.block_size = block_req.block.block_len;

    block_cache_reqs.emplace_back(BlockCacheReadReq{.key = key,
                                                    .option = option,
                                                    .io_buffer = IOBuffer(),
                                                    .buf_pos = buf_pos,
                                                    .block_req = block_req});
  }

  ReaderSharedState shared;
  shared.total = block_cache_reqs.size();
  shared.num_done = 0;
  shared.status = Status::OK();

  for (auto& block_cache_req : block_cache_reqs) {
    hub_->GetBlockCache()->AsyncRange(
        cache::NewContext(), block_cache_req.key,
        block_cache_req.block_req.block_offset, block_cache_req.block_req.len,
        &block_cache_req.io_buffer,
        [this, &block_cache_req, &shared](Status s) {
          BlockReadCallback(this, block_cache_req, shared, s);
        },
        block_cache_req.option);
  }

  Status ret;
  {
    std::unique_lock<std::mutex> lock(shared.mtx);
    while (shared.num_done < shared.total) {
      shared.cv.wait(lock);
    }

    ret = shared.status;
  }

  VLOG(4) << fmt::format("{} ChunkReader Read End", UUID());

  cb(ret);
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs
