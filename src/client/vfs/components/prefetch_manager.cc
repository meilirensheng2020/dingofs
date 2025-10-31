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

#include "prefetch_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>

#include "client/vfs/hub/vfs_hub.h"
#include "common/status.h"
#include "utils/executor/bthread/bthread_executor.h"
#include "utils/executor/executor.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace client {
namespace vfs {

using WriteLockGuard = dingofs::utils::WriteLockGuard;
using ReadLockGuard = dingofs::utils::ReadLockGuard;

Status PrefetchManager::Start(const uint32_t& threads,
                              const bool& use_bthread) {
  CHECK_GT(threads, 0);
  CHECK_NOTNULL(metrics_);

  bthread::ExecutionQueueOptions queue_options;
  queue_options.use_pthread = true;
  int rc = bthread::execution_queue_start(&task_queue_id_, &queue_options,
                                          &PrefetchManager::HandlePrefetchTask,
                                          this);
  if (rc != 0) {
    LOG(ERROR) << "Start execution queue failed: rc = " << rc;
    return Status::Internal("start execution queue failed.");
  }

  if (use_bthread) {
    prefetch_executor_ = std::make_unique<BthreadExecutor>(threads);
  } else {
    prefetch_executor_ = std::make_unique<ExecutorImpl>(threads);
  }

  auto ok = prefetch_executor_->Start();
  if (!ok) {
    LOG(ERROR) << "Start prefetch manager executor failed.";
    return Status::Internal("Start prefetch manager executor failed.");
  }

  block_cache_ = vfs_hub_->GetBlockCache();
  CHECK_NOTNULL(block_cache_);

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << fmt::format(
      "PrefetchManager started with {} executor, threads: {}",
      use_bthread ? "bthread" : "pthread", threads);

  return Status::OK();
}

Status PrefetchManager::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  if (bthread::execution_queue_stop(task_queue_id_) != 0) {
    LOG(ERROR) << "Stop execution queue failed.";
    return Status::Internal("stop execution queue failed");
  } else if (bthread::execution_queue_join(task_queue_id_) != 0) {
    LOG(ERROR) << "Join execution queue failed.";
    return Status::Internal("join execution queue failed");
  }

  auto ok = prefetch_executor_->Stop();
  if (!ok) {
    LOG(ERROR) << "Stop prefetch executor failed.";
    return Status::Internal("Stop prefetch executor failed.");
  }

  LOG(INFO) << "PrefetchManager stopped.";
  return Status::OK();
}

void PrefetchManager::SubmitTask(const BlockKey& key, size_t length) {
  CHECK_GT(length, 0);
  CHECK_EQ(0, bthread::execution_queue_execute(task_queue_id_,
                                               PrefetchTask(key, length)));
}

int PrefetchManager::HandlePrefetchTask(
    void* meta, bthread::TaskIterator<PrefetchTask>& iter) {
  if (iter.is_queue_stopped()) {
    return 0;
  }

  auto* self = static_cast<PrefetchManager*>(meta);

  for (; iter; ++iter) {
    auto& task = *iter;
    // Skip if the key is already being processed or exists in cache
    if (self->FilterOut(task)) {
      VLOG(12) << "Skip block: " << task.key.Filename()
               << ", length: " << task.length;
      continue;
    }

    self->SetBusy(task.key);
    // Run in thread pool
    self->AsyncPrefetch(task);
  }

  return 0;
}

void PrefetchManager::AsyncPrefetch(const PrefetchTask& task) {
  auto* self = this;
  prefetch_executor_->Execute([self, task]() { self->DoPrefetch(task); });
}

void PrefetchManager::DoPrefetch(const PrefetchTask& task) {
  cache::BlockKey key = task.key;
  size_t length = task.length;

  VLOG(6) << "Try to prefetch block : " << key.StoreKey()
          << ", length: " << length;

  auto status = block_cache_->Prefetch(cache::NewContext(), key, length);
  if (!status.ok()) {
    LOG_EVERY_N(WARNING, 100)
        << "Prefetch failed: "
        << "key = " << task.key.Filename() << ", length = " << length
        << ", status = " << status.ToString();
  }

  SetIdle(key);
}

bool PrefetchManager::IsBusy(const BlockKey& key) {
  ReadLockGuard lk(rwlock_);
  return inflight_keys_.count(key.Filename()) != 0;
}

void PrefetchManager::SetBusy(const BlockKey& key) {
  WriteLockGuard lk(rwlock_);
  inflight_keys_.insert(key.Filename());
  IncPrefetchBlocks();
}

void PrefetchManager::SetIdle(const BlockKey& key) {
  WriteLockGuard lk(rwlock_);
  inflight_keys_.erase(key.Filename());
  DecPrefetchBlocks();
}

bool PrefetchManager::FilterOut(const PrefetchTask& task) {
  return IsBusy(task.key) || block_cache_->IsCached(task.key);
}

void PrefetchManager::IncPrefetchBlocks() {
  metrics_->inflight_prefetch_blocks << 1;
}

void PrefetchManager::DecPrefetchBlocks() {
  metrics_->inflight_prefetch_blocks << -1;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs