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
 * Created Date: 2024-09-25
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_uploader.h"

#include <chrono>
#include <memory>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_manager.h"
#include "cache/utils/access_log.h"
#include "cache/utils/local_filesystem.h"
#include "cache/utils/phase_timer.h"
#include "options/cache/app.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using dingofs::cache::utils::LogIt;
using dingofs::utils::TaskThreadPool;
using dingofs::cache::utils::LogIt;
using dingofs::cache::utils::Phase;
using dingofs::cache::utils::PhaseTimer;
using dingofs::utils::TaskThreadPool;

BlockCacheUploader::BlockCacheUploader(
    blockaccess::BlockAccesser* block_accesser,
    std::shared_ptr<CacheStore> store, std::shared_ptr<Countdown> stage_count)
    : running_(false),
      block_accesser_(block_accesser),
      store_(store),
      stage_count_(stage_count) {
  scan_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("scan_stage_worker");
  upload_stage_thread_pool_ =
      std::make_unique<TaskThreadPool<>>("upload_stage_worker");
}

BlockCacheUploader::~BlockCacheUploader() { Shutdown(); }

void BlockCacheUploader::Init(uint64_t upload_workers,
                              uint64_t upload_queue_size) {
  if (!running_.exchange(true)) {
    // pending and uploading queue
    pending_queue_ = std::make_shared<PendingQueue>();
    uploading_queue_ = std::make_shared<UploadingQueue>(upload_queue_size);
    uploading_queue_->Start();

    // scan stage block worker
    CHECK(scan_stage_thread_pool_->Start(1) == 0);
    scan_stage_thread_pool_->Enqueue(&BlockCacheUploader::ScaningWorker, this);

    // upload stage block worker
    CHECK(upload_stage_thread_pool_->Start(upload_workers) == 0);
    for (uint64_t i = 0; i < upload_workers; i++) {
      upload_stage_thread_pool_->Enqueue(&BlockCacheUploader::UploadingWorker,
                                         this);
    }
  }
}

void BlockCacheUploader::Shutdown() {
  if (running_.exchange(false)) {
    uploading_queue_->Stop();
    scan_stage_thread_pool_->Stop();
    upload_stage_thread_pool_->Stop();
  }
}

void BlockCacheUploader::AddStageBlock(const BlockKey& key,
                                       const std::string& stage_path,
                                       BlockContext ctx) {
  StageBlock stage_block(key, stage_path, ctx);
  Staging(stage_block);
  pending_queue_->Push(stage_block);
}

// Reserve space for stage blocks which from |CTO_FLUSH|
bool BlockCacheUploader::CanUpload(const std::vector<StageBlock>& blocks) {
  if (blocks.empty()) {
    return false;
  }
  auto from = blocks[0].ctx.from;
  return from == BlockFrom::kCtoFlush ||
         uploading_queue_->Size() < uploading_queue_->Capacity() * 0.5;
}

void BlockCacheUploader::ScaningWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_blocks = pending_queue_->Pop(true);  // peek it
    if (!CanUpload(stage_blocks)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      continue;
    }

    stage_blocks = pending_queue_->Pop();
    for (const auto& stage_block : stage_blocks) {
      uploading_queue_->Push(stage_block);
    }
  }
}

void BlockCacheUploader::UploadingWorker() {
  while (running_.load(std::memory_order_relaxed)) {
    auto stage_block = uploading_queue_->Pop();
    if (stage_block.Valid()) {
      UploadStageBlock(stage_block);
    } else {
      LOG(WARNING) << "Abort invalid block(" << stage_block.key.Filename()
                   << "," << stage_block.seq_num << ").";
    }
  }
}

namespace {

void Log(const StageBlock& stage_block, size_t length, Status status,
         PhaseTimer timer) {
  auto message = StrFormat(
      "upload_stage(%s,%d): %s%s <%.6lf>", stage_block.key.Filename(), length,
      status.ToString(), timer.ToString(), timer.TotalUElapsed() / 1e6);
  LogIt(message);
}

};  // namespace

void BlockCacheUploader::UploadStageBlock(const StageBlock& stage_block) {
  Status status;
  PhaseTimer timer;
  std::shared_ptr<char> buffer;
  size_t length;
  auto defer = ::absl::MakeCleanup([&]() {
    if (!status.ok()) {
      Log(stage_block, length, status, timer);
    }
  });

  timer.NextPhase(Phase::kReadBlock);
  status = ReadBlock(stage_block, buffer, &length);
  if (status.ok()) {  // OK
    timer.NextPhase(Phase::kS3Put);
    UploadBlock(stage_block, buffer, length, timer);
  } else if (status.IsNotFound()) {  // already deleted
    Uploaded(stage_block, false);
  } else {  // throw error
    Uploaded(stage_block, false);
  }
}

Status BlockCacheUploader::ReadBlock(const StageBlock& stage_block,
                                     std::shared_ptr<char>& buffer,
                                     size_t* length) {
  auto stage_path = stage_block.stage_path;
  auto fs = LocalFileSystem();
  auto status =
      fs.ReadFile(stage_path, buffer, length, FLAGS_disk_cache_drop_page_cache);
  if (status.IsNotFound()) {
    LOG(ERROR) << "Stage block (path=" << stage_path
               << ") already deleted, abort upload!";
  } else if (!status.ok()) {
    LOG(ERROR) << "Read stage block (path=" << stage_path
               << ") failed: " << status.ToString() << ", abort upload!";
  }
  return status;
}

void BlockCacheUploader::UploadBlock(const StageBlock& stage_block,
                                     std::shared_ptr<char> buffer,
                                     size_t length, PhaseTimer timer) {
  auto retry_cb = [stage_block, buffer, length, timer, this](int code) {
    auto key = stage_block.key;
    if (code != 0) {
      LOG(ERROR) << "Upload object " << key.StoreKey()
                 << " failed, code=" << code;
      return true;  // retry
    }

    RemoveBlock(stage_block);
    Uploaded(stage_block, true);
    Log(stage_block, length, Status::OK(), timer);
    return false;
  };
  block_accesser_->AsyncPut(stage_block.key.StoreKey(), buffer.get(), length,
                            retry_cb);
}

void BlockCacheUploader::RemoveBlock(const StageBlock& stage_block) {
  auto status = store_->RemoveStage(stage_block.key, stage_block.ctx);
  if (!status.ok()) {
    LOG(WARNING) << "Remove stage block (path=" << stage_block.stage_path
                 << ") after upload failed: " << status.ToString();
  }
}

void BlockCacheUploader::Staging(const StageBlock& stage_block) {
  if (NeedCount(stage_block)) {
    stage_count_->Add(stage_block.key.ino, 1, false);
  }
}

void BlockCacheUploader::Uploaded(const StageBlock& stage_block, bool success) {
  if (NeedCount(stage_block)) {
    stage_count_->Add(stage_block.key.ino, -1, !success);
  }
}

bool BlockCacheUploader::NeedCount(const StageBlock& stage_block) {
  return stage_block.ctx.from == BlockFrom::kCtoFlush;
}

void BlockCacheUploader::WaitAllUploaded() {
  while (pending_queue_->Size() != 0 || uploading_queue_->Size() != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs
