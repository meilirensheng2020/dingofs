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

#ifndef DINGOFS_MDSV2_SERVER_H_
#define DINGOFS_MDSV2_SERVER_H_

#include <atomic>
#include <string>

#include "brpc/server.h"
#include "mdsv2/background/compaction.h"
#include "mdsv2/background/fsinfo_sync.h"
#include "mdsv2/background/heartbeat.h"
#include "mdsv2/background/mds_monitor.h"
#include "mdsv2/common/crontab.h"
#include "mdsv2/coordinator/coordinator_client.h"
#include "mdsv2/filesystem/filesystem.h"
#include "mdsv2/filesystem/quota.h"
#include "mdsv2/filesystem/renamer.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/storage/storage.h"
#include "utils/configuration.h"

namespace dingofs {
namespace mdsv2 {

using ::dingofs::utils::Configuration;

class Server {
 public:
  static Server& GetInstance();

  bool InitConfig(const std::string& path);

  bool InitLog();

  bool InitMDSMeta();

  bool InitCoordinatorClient(const std::string& coor_url);

  bool InitStorage(const std::string& store_url);

  bool InitQuotaProcessor();

  bool InitRenamer();

  bool InitMutationMerger();

  bool InitFileSystem();

  bool InitHeartbeat();

  bool InitFsInfoSync();

  bool InitWorkerSet();

  bool InitMDSMonitor();

  bool InitCompactChunkProcessor();

  bool InitCrontab();

  std::string GetPidFilePath();
  std::string GetListenAddr();
  MDSMeta& GetMDSMeta();
  MDSMetaMapPtr GetMDSMetaMap();
  KVStoragePtr GetKVStorage();
  HeartbeatPtr GetHeartbeat() { return heartbeat_; }
  FsInfoSync& GetFsInfoSync() { return fs_info_sync_; }
  CoordinatorClientPtr GetCoordinatorClient() { return coordinator_client_; }
  FileSystemSetPtr GetFileSystemSet() { return file_system_set_; }
  MDSMonitorPtr GetMDSMonitor() { return mds_monitor_; }
  CompactChunkProcessorPtr GetCompactChunkProcessor() { return compact_chunk_processor_; }
  MutationProcessorPtr GetMutationProcessor() { return mutation_processor_; }

  void Run();

  void Stop();

 private:
  explicit Server() = default;
  ~Server();

  std::atomic<bool> stop_{false};
  brpc::Server brpc_server_;

  Configuration conf_;

  // mds self info
  MDSMeta mds_meta_;
  // all cluster mds
  MDSMetaMapPtr mds_meta_map_;

  // This is manage crontab, like heartbeat.
  CrontabManager crontab_manager_;
  // Crontab config
  std::vector<CrontabConfig> crontab_configs_;

  // coordinator client
  CoordinatorClientPtr coordinator_client_;

  // backend kv storage
  KVStoragePtr kv_storage_;

  // quota processor
  QuotaProcessorPtr quota_processor_;

  // renamer
  RenamerPtr renamer_;

  // mutation merger
  MutationProcessorPtr mutation_processor_;

  // filesystem
  FileSystemSetPtr file_system_set_;

  // heartbeat to coordinator
  HeartbeatPtr heartbeat_;

  // fs info sync
  FsInfoSync fs_info_sync_;

  // mds monitor
  MDSMonitorPtr mds_monitor_;

  // compaction
  CompactChunkProcessorPtr compact_chunk_processor_;

  // worker set for service request
  WorkerSetPtr read_worker_set_;
  WorkerSetPtr write_worker_set_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVER_H_