// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/background/gc.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "blockaccess/block_accesser_factory.h"
#include "blockaccess/rados/rados_common.h"
#include "blockaccess/s3/s3_common.h"
#include "cache/blockcache/cache_store.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "mdsv2/common/codec.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/runnable.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/tracing.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

DECLARE_uint32(fs_scan_batch_size);

DEFINE_uint32(gc_worker_num, 4, "gc worker set num");
DEFINE_uint32(gc_max_pending_task_count, 8192, "gc max pending task count");

DEFINE_uint32(gc_delfile_reserve_time_s, 600, "gc del file reserve time");

DEFINE_uint32(gc_filesession_reserve_time_s, 86400, "gc file session reserve time");

static const std::string kWorkerSetName = "GC";

void CleanDelSliceTask::Run() {
  auto status = CleanDelSlice();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice] clean deleted slice fail, {}", status.error_str());
  }
}

Status CleanDelSliceTask::CleanDelSlice() {
  // delete data from s3
  std::string slice_id_trace;
  auto trash_slice_list = MetaCodec::DecodeDelSliceValue(value_);
  for (size_t i = 0; i < trash_slice_list.slices_size(); ++i) {
    const auto& slice = trash_slice_list.slices().at(i);

    uint64_t chunk_offset = slice.chunk_index() * slice.chunk_size();
    for (const auto& range : slice.ranges()) {
      for (uint64_t len = 0; len < range.len(); len += slice.block_size()) {
        uint64_t block_index = (range.offset() - chunk_offset + len) / slice.block_size();
        cache::BlockKey block_key(slice.fs_id(), slice.ino(), slice.slice_id(), block_index, 0);

        DINGO_LOG(INFO) << fmt::format("[gc.delslice] delete block filename({}) key({}).", block_key.Filename(),
                                       block_key.StoreKey());
        auto status = data_accessor_->Delete(block_key.StoreKey());
        if (!status.ok()) {
          return Status(pb::error::EINTERNAL, fmt::format("delete s3 object fail, {}", status.ToString()));
        }
      }
    }

    slice_id_trace += std::to_string(slice.slice_id());
    if (i + 1 < trash_slice_list.slices_size()) {
      slice_id_trace += ",";
    }
  }

  // delete slice
  class Trace trace;
  CleanDelSliceOperation operation(trace, key_);
  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delslice] clean slice finish, slice({}).", slice_id_trace);

  return status;
}

void CleanDelFileTask::Run() {
  auto status = CleanDelFile(attr_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] clean delfile fail, {}", status.error_str());
  }
}

Status CleanDelFileTask::GetChunks(uint32_t fs_id, Ino ino, std::vector<ChunkType>& chunks) {
  class Trace trace;
  ScanChunkOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  auto& result = operation.GetResult();

  chunks = std::move(result.chunks);

  return Status::OK();
}

Status CleanDelFileTask::CleanDelFile(const AttrType& attr) {
  DINGO_LOG(INFO) << fmt::format("[gc.delfile] clean delfile, ino({}) nlink({}) len({}) version({}).", attr.ino(),
                                 attr.nlink(), attr.length(), attr.version());
  // get file chunks
  std::vector<ChunkType> chunks;
  auto status = GetChunks(attr.fs_id(), attr.ino(), chunks);
  if (!status.ok()) {
    return status;
  }

  // delete data from s3
  for (const auto& chunk : chunks) {
    uint64_t chunk_offset = chunk.index() * chunk.chunk_size();
    for (const auto& slice : chunk.slices()) {
      for (uint64_t len = 0; len < slice.len(); len += chunk.block_size()) {
        uint64_t block_index = (slice.offset() - chunk_offset + len) / chunk.block_size();
        cache::BlockKey block_key(attr.fs_id(), attr.ino(), slice.id(), block_index, chunk.version());

        DINGO_LOG(INFO) << fmt::format("[gc.delfile] delete block filename({}) key({}).", block_key.Filename(),
                                       block_key.StoreKey());
        auto status = data_accessor_->Delete(block_key.StoreKey());
        if (!status.ok()) {
          return Status(pb::error::EINTERNAL, fmt::format("delete s3 object fail, {}", status.ToString()));
        }
      }
    }
  }

  // delete inode
  class Trace trace;
  CleanDelFileOperation operation(trace, attr.fs_id(), attr.ino());
  status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  DINGO_LOG(INFO) << fmt::format("[gc.delfile] clean file({}/{}) finish.", attr.fs_id(), attr.ino());

  return Status::OK();
}

void CleanExpiredFileSessionTask::Run() {
  auto status = CleanExpiredFileSession();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.filesession] clean filesession fail, {}", status.error_str());
  }
}

Status CleanExpiredFileSessionTask::CleanExpiredFileSession() {
  class Trace trace;
  DeleteFileSessionOperation operation(trace, file_sessions_);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

bool GcProcessor::Init() {
  CHECK(dist_lock_ != nullptr) << "dist lock is nullptr.";

  if (!dist_lock_->Init()) {
    DINGO_LOG(ERROR) << "[gc] init dist lock fail.";
    return false;
  }

  worker_set_ = ExecqWorkerSet::New(kWorkerSetName, FLAGS_gc_worker_num, FLAGS_gc_max_pending_task_count);
  return worker_set_->Init();
}

void GcProcessor::Destroy() {
  if (dist_lock_ != nullptr) {
    dist_lock_->Destroy();
  }

  if (worker_set_ != nullptr) {
    worker_set_->Destroy();
  }
}

void GcProcessor::Run() {
  auto status = LaunchGc();
  if (!status.ok()) {
    DINGO_LOG(INFO) << fmt::format("[gc] run gc, {}.", status.error_str());
  }
}

Status GcProcessor::ManualCleanDelSlice(Trace& trace, uint32_t fs_id, Ino ino, uint64_t chunk_index) {
  auto block_accessor = GetOrCreateDataAccesser(fs_id);
  if (block_accessor == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
    return Status(pb::error::EINTERNAL, "get data accesser fail");
  }

  ScanDelSliceOperation operation(
      trace, fs_id, ino, chunk_index, [&](const std::string& key, const std::string& value) -> bool {
        auto task = CleanDelSliceTask::New(operation_processor_, block_accessor, key, value);
        auto status = task->CleanDelSlice();
        if (!status.ok()) {
          LOG(ERROR) << fmt::format("[gc.delslice] clean delfile fail, {}.", status.error_str());
          return false;  // stop scanning on error
        }

        return true;
      });

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delslice] scan delslice fail, {}.", status.error_str());
    return status;
  }

  return status;
}

Status GcProcessor::ManualCleanDelFile(Trace& trace, uint32_t fs_id, Ino ino) {
  auto block_accessor = GetOrCreateDataAccesser(fs_id);
  if (block_accessor == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
    return Status(pb::error::EINTERNAL, "get data accesser fail");
  }

  GetDelFileOperation operation(trace, fs_id, ino);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] get delfile fail, fs_id({}) ino({}).", fs_id, ino);
    return status;
  }

  auto& result = operation.GetResult();
  const auto& attr = result.attr;

  auto task = CleanDelFileTask::New(operation_processor_, block_accessor, attr);
  status = task->CleanDelFile(attr);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.delfile] clean delfile fail, {}.", status.error_str());
    return status;
  }

  return Status::OK();
}

Status GcProcessor::LaunchGc() {
  bool running = false;
  if (!is_running_.compare_exchange_strong(running, true)) {
    return Status(pb::error::EINTERNAL, "gc already running");
  }

  DEFER(is_running_.store(false));

  if (!dist_lock_->IsLocked()) {
    return Status(pb::error::EINTERNAL, "not own lock");
  }

  auto fses = file_system_set_->GetAllFileSystem();

  // delslice
  for (auto& fs : fses) {
    ScanDelSlice(fs->FsId());
  }

  // delfile
  for (auto& fs : fses) {
    ScanDelFile(fs->FsId());
  }

  // filesession
  for (auto& fs : fses) {
    ScanExpiredFileSession(fs->FsId());
  }
  return Status::OK();
}

bool GcProcessor::Execute(TaskRunnablePtr task) {
  if (!worker_set_->ExecuteLeastQueue(task)) {
    DINGO_LOG(ERROR) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

bool GcProcessor::Execute(Ino ino, TaskRunnablePtr task) {
  if (!worker_set_->ExecuteHash(ino, task)) {
    DINGO_LOG(ERROR) << "[gc] execute task fail.";
    return false;
  }
  return true;
}

Status GcProcessor::GetClientList(std::set<std::string>& clients) {
  Trace trace;
  ScanClientOperation operation(trace);

  auto status = operation_processor_->RunAlone(&operation);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] get client list fail, error({}).", status.error_str());
    return status;
  }

  auto& result = operation.GetResult();
  for (auto& client : result.client_entries) {
    clients.insert(client.id());
  }

  return Status::OK();
}

void GcProcessor::ScanDelSlice(uint32_t fs_id) {
  Trace trace;
  uint32_t count = 0, exec_count = 0;
  ScanDelSliceOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    ++count;

    uint32_t fs_id = 0;
    Ino ino = 0;
    uint64_t chunk_index, time_ns;
    MetaCodec::DecodeDelSliceKey(key, fs_id, ino, chunk_index, time_ns);
    CHECK(fs_id > 0) << "invalid fs id.";
    CHECK(ino > 0) << "invalid ino.";

    auto block_accessor = GetOrCreateDataAccesser(fs_id);
    if (block_accessor == nullptr) {
      LOG(ERROR) << fmt::format("[gc.delslice] get data accesser fail, fs_id({}).", fs_id);
      return true;
    }

    ++exec_count;
    return Execute(ino, CleanDelSliceTask::New(operation_processor_, block_accessor, key, value));
  });

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delslice] scan delslice count({}/{}), status({}).", exec_count, count,
                                 status.error_str());
}

void GcProcessor::ScanDelFile(uint32_t fs_id) {
  Trace trace;
  uint32_t count = 0, exec_count = 0;
  ScanDelFileOperation operation(trace, fs_id, [&](const std::string& key, const std::string& value) -> bool {
    ++count;

    uint32_t fs_id = 0;
    Ino ino = 0;
    MetaCodec::DecodeDelFileKey(key, fs_id, ino);
    CHECK(fs_id > 0) << "invalid fs id.";
    CHECK(ino > 0) << "invalid ino.";

    auto block_accessor = GetOrCreateDataAccesser(fs_id);
    if (block_accessor == nullptr) {
      LOG(ERROR) << fmt::format("[gc.delfile] get data accesser fail, fs_id({}).", fs_id);
      return true;
    }

    auto attr = MetaCodec::DecodeDelFileValue(value);
    if (ShouldDeleteFile(attr)) {
      ++exec_count;
      return Execute(CleanDelFileTask::New(operation_processor_, block_accessor, attr));
    }

    return true;
  });

  auto status = operation_processor_->RunAlone(&operation);

  DINGO_LOG(INFO) << fmt::format("[gc.delfile] scan delfile count({}/{}), status({}).", exec_count, count,
                                 status.error_str());
}

void GcProcessor::ScanExpiredFileSession(uint32_t fs_id) {
  // get alive clients
  // to dead clients, we will clean their file sessions
  std::set<std::string> alive_clients;
  auto status = GetClientList(alive_clients);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[gc.filesession] get client list fail, {}.", status.error_str());
    return;
  }

  Trace trace;
  uint32_t count = 0, exec_count = 0;
  std::vector<FileSessionEntry> file_sessions;
  ScanFileSessionOperation operation(trace, fs_id, [&](const FileSessionEntry& file_session) -> bool {
    ++count;

    if (ShouldCleanFileSession(file_session, alive_clients)) {
      file_sessions.push_back(file_session);
    }

    if (file_sessions.size() >= FLAGS_fs_scan_batch_size) {
      exec_count += file_sessions.size();
      if (!Execute(CleanExpiredFileSessionTask::New(operation_processor_, file_sessions))) {
        file_sessions.clear();
        return false;
      }
      file_sessions.clear();
    }

    return true;
  });

  status = operation_processor_->RunAlone(&operation);

  if (!file_sessions.empty()) {
    exec_count += file_sessions.size();
    Execute(CleanExpiredFileSessionTask::New(operation_processor_, file_sessions));
  }

  DINGO_LOG(INFO) << fmt::format("[gc.filesession] scan file session count({}/{}), status({}).", exec_count, count,
                                 status.error_str());
}

bool GcProcessor::ShouldDeleteFile(const AttrType& attr) {
  uint64_t now_s = Helper::Timestamp();
  return (attr.ctime() / 1000000000 + FLAGS_gc_delfile_reserve_time_s) < now_s;
}

bool GcProcessor::ShouldCleanFileSession(const FileSessionEntry& file_session,
                                         const std::set<std::string>& alive_clients) {
  // check whether client exist
  if (alive_clients.count(file_session.client_id()) == 0) {
    return true;
  }

  uint64_t now_s = Helper::Timestamp();
  return file_session.create_time_s() + FLAGS_gc_filesession_reserve_time_s < now_s;
}

blockaccess::BlockAccesserSPtr GcProcessor::GetOrCreateDataAccesser(uint32_t fs_id) {
  auto it = block_accessers_.find(fs_id);
  if (it != block_accessers_.end()) {
    return it->second;
  }

  auto fs = file_system_set_->GetFileSystem(fs_id);
  if (fs == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[gc] get filesystem({}) fail.", fs_id);
    return nullptr;
  }

  const auto fs_info = fs->GetFsInfo();

  blockaccess::BlockAccessOptions options;
  if (fs_info.fs_type() == pb::mdsv2::FsType::S3) {
    const auto& s3_info = fs_info.extra().s3_info();
    if (s3_info.ak().empty() || s3_info.sk().empty() || s3_info.endpoint().empty() || s3_info.bucketname().empty()) {
      DINGO_LOG(ERROR) << fmt::format("[gc] get s3 info fail, fs_id({}) s3_info({}).", fs_id,
                                      s3_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kS3;
    options.s3_options.s3_info = blockaccess::S3Info{
        .ak = s3_info.ak(), .sk = s3_info.sk(), .endpoint = s3_info.endpoint(), .bucket_name = s3_info.bucketname()};

  } else {
    const auto& rados_info = fs_info.extra().rados_info();
    if (rados_info.mon_host().empty() || rados_info.user_name().empty() || rados_info.key().empty() ||
        rados_info.pool_name().empty()) {
      DINGO_LOG(ERROR) << fmt::format("[gc] get rados info fail, fs_id({}) rados_info({}).", fs_id,
                                      rados_info.ShortDebugString());
      return nullptr;
    }

    options.type = blockaccess::AccesserType::kRados;
    options.rados_options = blockaccess::RadosOptions{.mon_host = rados_info.mon_host(),
                                                      .user_name = rados_info.user_name(),
                                                      .key = rados_info.key(),
                                                      .pool_name = rados_info.pool_name(),
                                                      .cluster_name = rados_info.cluster_name()};
  }

  blockaccess::BlockAccesserFactory factory;
  auto block_accessor = factory.NewShareBlockAccesser(options);
  auto status = block_accessor->Init();
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("[gc] init block accesser fail, status({}).", status.ToString());
    return nullptr;
  }

  block_accessers_[fs_id] = block_accessor;

  return block_accessor;
}

}  // namespace mdsv2
}  // namespace dingofs