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
 * Project: dingo
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include "metaserver/metaserver.h"

#include <braft/builtin_service_impl.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "blockaccess/accesser_common.h"
#include "blockaccess/s3/aws/s3_adapter.h"
#include "common/version.h"
#include "metaserver/common/dynamic_config.h"
#include "metaserver/common/types.h"
#include "metaserver/compaction/s3compact_manager.h"
#include "metaserver/copyset/copyset_service.h"
#include "metaserver/metaserver_service.h"
#include "metaserver/register.h"
#include "metaserver/resource_statistic.h"
#include "metaserver/storage/rocksdb_options.h"
#include "metaserver/storage/rocksdb_perf.h"
#include "metaserver/trash/trash_manager.h"
#include "utils/crc32.h"
#include "utils/string_util.h"
#include "utils/uri_parser.h"

namespace braft {

DECLARE_bool(raft_sync);
DECLARE_bool(raft_sync_meta);
DECLARE_bool(raft_sync_segments);
DECLARE_bool(raft_use_fsync_rather_than_fdatasync);
DECLARE_int32(raft_max_install_snapshot_tasks_num);

}  // namespace braft

namespace brpc {
DECLARE_bool(graceful_quit_on_sigterm);
}  // namespace brpc

namespace dingofs {
namespace metaserver {

using copyset::CopysetNodeManager;
using copyset::CopysetServiceImpl;
using copyset::RaftCliService2;
using fs::FileSystemType;
using fs::LocalFileSystemOption;
using fs::LocalFsFactory;
using storage::StorageOptions;
using utils::Configuration;

using stub::rpcclient::ChannelManager;
using stub::rpcclient::Cli2ClientImpl;
using stub::rpcclient::MDSBaseClient;
using stub::rpcclient::MdsClient;
using stub::rpcclient::MdsClientImpl;
using stub::rpcclient::MetaCache;
using stub::rpcclient::MetaServerClientImpl;

using dingofs::metaserver::MetaServerID;

USING_FLAG(superpartition_access_logging);

void Metaserver::InitOptions(std::shared_ptr<Configuration> conf) {
  conf_ = conf;
  conf_->GetValueFatalIfFail("global.ip", &options_.ip);
  conf_->GetValueFatalIfFail("global.port", &options_.port);
  conf_->GetValueFatalIfFail("global.external_ip", &options_.externalIp);
  conf_->GetValueFatalIfFail("global.external_port", &options_.externalPort);
  conf_->GetBoolValue("global.enable_external_server",
                      &options_.enableExternalServer);
  conf_->GetBoolValue("global.superpartition_access_logging",
                      &FLAGS_superpartition_access_logging);

  LOG(INFO) << "Init metaserver option, options_.ip = " << options_.ip
            << ", options_.port = " << options_.port
            << ", options_.externalIp = " << options_.externalIp
            << ", options_.externalPort = " << options_.externalPort
            << ", options_.enableExternalServer = "
            << options_.enableExternalServer;

  std::string value;
  conf_->GetValueFatalIfFail("bthread.worker_count", &value);
  if (value == "auto") {
    options_.bthreadWorkerCount = -1;
  } else if (!dingofs::utils::StringToInt(value,
                                          &options_.bthreadWorkerCount)) {
    LOG(WARNING) << "Parse bthread.worker_count to int failed, string value: "
                 << value;
  }

  InitBRaftFlags(conf);
}

void Metaserver::InitRegisterOptions() {
  conf_->GetValueFatalIfFail("mds.listen.addr",
                             &registerOptions_.mdsListenAddr);
  conf_->GetValueFatalIfFail("global.ip",
                             &registerOptions_.metaserverInternalIp);
  conf_->GetValueFatalIfFail("global.external_ip",
                             &registerOptions_.metaserverExternalIp);
  conf_->GetValueFatalIfFail("global.port",
                             &registerOptions_.metaserverInternalPort);
  conf_->GetValueFatalIfFail("global.external_port",
                             &registerOptions_.metaserverExternalPort);
  conf_->GetValueFatalIfFail("mds.register_retries",
                             &registerOptions_.registerRetries);
  conf_->GetValueFatalIfFail("mds.register_timeoutMs",
                             &registerOptions_.registerTimeout);
}

void Metaserver::InitLocalFileSystem() {
  LocalFileSystemOption option;

  localFileSystem_ = LocalFsFactory::CreateFs(FileSystemType::EXT4, "");
  LOG_IF(FATAL, 0 != localFileSystem_->Init(option))
      << "Failed to initialize local filesystem";
}

void InitS3Option(const std::shared_ptr<Configuration>& conf,
                  S3ClientAdaptorOption* s3Opt) {
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.batchsize", &s3Opt->batchSize));
  LOG_IF(FATAL, !conf->GetBoolValue("s3.enableDeleteObjects",
                                    &s3Opt->enableDeleteObjects));
}

void Metaserver::InitPartitionOptionFromConf(
    PartitionCleanOption* partion_clean_option) {
  LOG_IF(FATAL, !conf_->GetUInt32Value("partition.clean.scanPeriodSec",
                                       &partion_clean_option->scanPeriodSec));
  LOG_IF(FATAL,
         !conf_->GetUInt32Value("partition.clean.inodeDeletePeriodMs",
                                &partion_clean_option->inodeDeletePeriodMs));
}

void Metaserver::InitRecycleManagerOption(
    RecycleManagerOption* recycleManagerOption) {
  recycleManagerOption->mdsClient = mdsClient_;
  recycleManagerOption->metaClient = metaClient_;
  LOG_IF(FATAL, !conf_->GetUInt32Value("recycle.manager.scanPeriodSec",
                                       &recycleManagerOption->scanPeriodSec));
  LOG_IF(FATAL, !conf_->GetUInt32Value("recycle.cleaner.scanLimit",
                                       &recycleManagerOption->scanLimit));
}

void InitExcutorOption(const std::shared_ptr<Configuration>& conf,
                       stub::common::ExcutorOpt* opts, bool internal) {
  if (internal) {
    conf->GetValueFatalIfFail("excutorOpt.maxInternalRetry", &opts->maxRetry);
  } else {
    conf->GetValueFatalIfFail("excutorOpt.maxRetry", &opts->maxRetry);
  }

  conf->GetValueFatalIfFail("excutorOpt.retryIntervalUS",
                            &opts->retryIntervalUS);
  conf->GetValueFatalIfFail("excutorOpt.rpcTimeoutMS", &opts->rpcTimeoutMS);
  conf->GetValueFatalIfFail("excutorOpt.rpcStreamIdleTimeoutMS",
                            &opts->rpcStreamIdleTimeoutMS);
  conf->GetValueFatalIfFail("excutorOpt.maxRPCTimeoutMS",
                            &opts->maxRPCTimeoutMS);
  conf->GetValueFatalIfFail("excutorOpt.maxRetrySleepIntervalUS",
                            &opts->maxRetrySleepIntervalUS);
  conf->GetValueFatalIfFail("excutorOpt.minRetryTimesForceTimeoutBackoff",
                            &opts->minRetryTimesForceTimeoutBackoff);
  conf->GetValueFatalIfFail("excutorOpt.maxRetryTimesBeforeConsiderSuspend",
                            &opts->maxRetryTimesBeforeConsiderSuspend);
  conf->GetValueFatalIfFail("excutorOpt.batchInodeAttrLimit",
                            &opts->batchInodeAttrLimit);
  conf->GetValueFatalIfFail("excutorOpt.enableMultiMountPointRename",
                            &opts->enableRenameParallel);
}

void InitMetaCacheOption(const std::shared_ptr<Configuration>& conf,
                         stub::common::MetaCacheOpt* opts) {
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRetry",
                            &opts->metacacheGetLeaderRetry);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheRPCRetryIntervalUS",
                            &opts->metacacheRPCRetryIntervalUS);
  conf->GetValueFatalIfFail("metaCacheOpt.metacacheGetLeaderRPCTimeOutMS",
                            &opts->metacacheGetLeaderRPCTimeOutMS);
}

void Metaserver::Init() {
  TrashOption trash_option;
  trash_option.InitTrashOptionFromConf(conf_);

  // init mds client
  mdsBase_ = new MDSBaseClient();
  ::dingofs::stub::common::InitMdsOption(conf_.get(), &mdsOptions_);
  mdsClient_ = std::make_shared<MdsClientImpl>();
  mdsClient_->Init(mdsOptions_, mdsBase_);

  // init metaserver client for recycle
  InitMetaClient();

  block_accesser_factory_ =
      std::make_shared<blockaccess::BlockAccesserFactory>();

  S3ClientAdaptorOption s3_client_adaptor_option;
  InitS3Option(conf_, &s3_client_adaptor_option);

  // read aws sdk relate param from conf file
  blockaccess::BlockAccessOptions block_access_options;
  blockaccess::InitAwsSdkConfig(
      conf_.get(), &block_access_options.s3_options.aws_sdk_config);
  blockaccess::InitBlockAccesserThrottleOptions(
      conf_.get(), &block_access_options.throttle_options);

  {
    //  related to trash
    trash_option.block_access_options = block_access_options;
    trash_option.block_accesser_factory = block_accesser_factory_;
    trash_option.s3_client_adaptor_option = s3_client_adaptor_option;
    trash_option.mdsClient = mdsClient_;
    TrashManager::GetInstance().Init(trash_option);
  }

  RecycleManagerOption recycleManagerOption;
  InitRecycleManagerOption(&recycleManagerOption);
  RecycleManager::GetInstance().Init(recycleManagerOption);

  // NOTE: Do not arbitrarily adjust the order, there are dependencies
  //       between different modules
  InitLocalFileSystem();
  InitStorage();
  InitCopysetNodeManager();

  // get metaserver id and token before heartbeat
  GetMetaserverDataByLoadOrRegister();
  InitResourceCollector();
  InitHeartbeat();
  InitInflightThrottle();

  S3CompactManager::GetInstance().Init(conf_);

  {
    // related to partition clean
    PartitionCleanOption partition_clean_option;
    InitPartitionOptionFromConf(&partition_clean_option);
    partition_clean_option.block_access_options = block_access_options;
    partition_clean_option.block_accesser_factory = block_accesser_factory_;
    partition_clean_option.s3_client_adaptor_option = s3_client_adaptor_option;
    partition_clean_option.mdsClient = mdsClient_;

    PartitionCleanManager::GetInstance().Init(partition_clean_option);
  }

  conf_->ExposeMetric("dingofs_metaserver_config");
  inited_ = true;
}

void Metaserver::InitMetaClient() {
  metaClient_ = std::make_shared<MetaServerClientImpl>();
  auto cli2Client = std::make_shared<Cli2ClientImpl>();
  auto metaCache = std::make_shared<MetaCache>();
  stub::common::MetaCacheOpt metaCacheOpt;
  InitMetaCacheOption(conf_, &metaCacheOpt);
  metaCache->Init(metaCacheOpt, cli2Client, mdsClient_);
  auto channelManager = std::make_shared<ChannelManager<MetaServerID>>();
  stub::common::ExcutorOpt excutorOpt;
  stub::common::ExcutorOpt internalOpt;
  InitExcutorOption(conf_, &excutorOpt, false);
  InitExcutorOption(conf_, &internalOpt, true);
  metaClient_->Init(excutorOpt, internalOpt, metaCache, channelManager);
}

void Metaserver::GetMetaserverDataByLoadOrRegister() {
  std::string metaFilePath;
  conf_->GetValueFatalIfFail("metaserver.meta_file_path", &metaFilePath);
  if (localFileSystem_->FileExists(metaFilePath)) {
    // get metaserver from load
    LOG_IF(FATAL, LoadMetaserverMeta(metaFilePath, &metadata_) != 0)
        << "load metaserver meta fail, path = " << metaFilePath;
  } else {
    // register metaserver to mds
    InitRegisterOptions();
    Register registerMDS(registerOptions_);
    LOG(INFO) << "register metaserver to mds";
    LOG_IF(FATAL, registerMDS.RegisterToMDS(&metadata_) != 0)
        << "Failed to register metaserver to MDS.";

    LOG_IF(FATAL, PersistMetaserverMeta(metaFilePath, &metadata_) != 0)
        << "persist metadata meta to file fail, path = " << metaFilePath;
    LOG(INFO) << "metaserver " << metadata_.ShortDebugString();
  }
}

int Metaserver::PersistMetaserverMeta(
    std::string path, pb::metaserver::MetaServerMetadata* metadata) {
  std::string tempData;
  metadata->set_checksum(0);
  bool ret = metadata->SerializeToString(&tempData);
  if (!ret) {
    LOG(ERROR) << "convert MetaServerMetadata to string fail";
    return -1;
  }

  uint32_t crc = dingofs::utils::CRC32(0, tempData.c_str(), tempData.length());
  metadata->set_checksum(crc);

  std::string data;
  ret = metadata->SerializeToString(&data);
  if (!ret) {
    LOG(ERROR) << "convert MetaServerMetadata to string fail";
    return -1;
  }

  return PersistDataToLocalFile(localFileSystem_, path, data);
}

int Metaserver::LoadMetaserverMeta(
    const std::string& metaFilePath,
    pb::metaserver::MetaServerMetadata* metadata) {
  std::string data;
  int ret = LoadDataFromLocalFile(localFileSystem_, metaFilePath, &data);
  if (ret != 0) {
    LOG(ERROR) << "load metaserver meta from file fail, path = "
               << metaFilePath;
    return ret;
  }

  LOG(INFO) << "load data from file, path = " << metaFilePath
            << ", len = " << data.length() << ", data = " << data;

  bool ret1 = metadata->ParseFromString(data);
  if (!ret1) {
    LOG(ERROR) << "parse metaserver meta from string fail, data = " << data;
    return -1;
  }

  uint32_t crcFromFile = metadata->checksum();
  std::string tempData;
  metadata->set_checksum(0);
  bool ret2 = metadata->SerializeToString(&tempData);
  if (!ret2) {
    LOG(ERROR) << "convert MetaServerMetadata to string fail";
    return -1;
  }

  uint32_t crc = dingofs::utils::CRC32(0, tempData.c_str(), tempData.length());
  if (crc != crcFromFile) {
    LOG(ERROR) << "crc is mismatch";
    return -1;
  }

  return 0;
}

int Metaserver::LoadDataFromLocalFile(std::shared_ptr<fs::LocalFileSystem> fs,
                                      const std::string& localPath,
                                      std::string* data) {
  if (!fs->FileExists(localPath)) {
    LOG(ERROR) << "get data from local file fail, path = " << localPath;
    return -1;
  }

  int fd = fs->Open(localPath.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Fail to open local file for write, path = " << localPath;
    return -1;
  }

#define METAFILE_MAX_SIZE 4096
  char buf[METAFILE_MAX_SIZE];
  int readCount = fs->Read(fd, buf, 0, METAFILE_MAX_SIZE);
  if (readCount <= 0) {
    LOG(ERROR) << "Failed to read data from file, path = " << localPath
               << ", readCount = " << readCount;
    return -1;
  }

  if (fs->Close(fd)) {
    LOG(ERROR) << "Failed to close file, path = " << localPath;
    return -1;
  }

  *data = std::string(buf, readCount);
  return 0;
}

int Metaserver::PersistDataToLocalFile(std::shared_ptr<fs::LocalFileSystem> fs,
                                       const std::string& localPath,
                                       const std::string& data) {
  LOG(INFO) << "persist data to file, path  = " << localPath
            << ", data len = " << data.length() << ", data = " << data;
  int fd = fs->Open(localPath.c_str(), O_RDWR | O_CREAT);
  if (fd < 0) {
    LOG(ERROR) << "Fail to open local file for write, path = " << localPath;
    return -1;
  }

  int writtenCount = fs->Write(fd, data.c_str(), 0, data.size());
  if (writtenCount < 0 || static_cast<size_t>(writtenCount) != data.size()) {
    LOG(ERROR) << "Failed to write data to file, path = " << localPath
               << ", writtenCount = " << writtenCount
               << ", data size = " << data.size();
    return -1;
  }

  if (fs->Close(fd)) {
    LOG(ERROR) << "Failed to close file, path = " << localPath;
    return -1;
  }

  return 0;
}

void Metaserver::Run() {
  if (!inited_) {
    LOG(ERROR) << "Metaserver not inited yet!";
    return;
  }

  TrashManager::GetInstance().Run();

  RecycleManager::GetInstance().Run();

  // start heartbeat
  LOG_IF(FATAL, heartbeat_.Run() != 0) << "Failed to start heartbeat manager.";

  // set metaserver version in metric
  dingofs::ExposeDingoVersion();

  PartitionCleanManager::GetInstance().Run();

  // add internal server
  server_ = absl::make_unique<brpc::Server>();
  metaService_ = absl::make_unique<MetaServerServiceImpl>(
      copysetNodeManager_, inflightThrottle_.get());
  copysetService_ = absl::make_unique<CopysetServiceImpl>(copysetNodeManager_);
  raftCliService2_ = absl::make_unique<RaftCliService2>(copysetNodeManager_);

  // add metaserver service
  LOG_IF(FATAL, server_->AddService(metaService_.get(),
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
      << "add metaserverService error";
  LOG_IF(FATAL, server_->AddService(copysetService_.get(),
                                    brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
      << "add copysetservice error";

  butil::ip_t ip;
  LOG_IF(FATAL, 0 != butil::str2ip(options_.ip.c_str(), &ip))
      << "convert " << options_.ip << " to ip failed";
  butil::EndPoint listenAddr(ip, options_.port);

  // add raft-related service
  copysetNodeManager_->AddService(server_.get(), listenAddr);

  // start internal rpc server
  brpc::ServerOptions option;
  if (options_.bthreadWorkerCount != -1) {
    option.num_threads = options_.bthreadWorkerCount;
  }
  LOG_IF(FATAL, server_->Start(listenAddr, &option) != 0)
      << "start internal brpc server error";

  // add external server
  if (options_.enableExternalServer) {
    LOG(INFO) << "metaserver enable external server, options_.externalIp = "
              << options_.externalIp
              << ", options_.externalPort = " << options_.externalPort;
    externalServer_ = absl::make_unique<brpc::Server>();
    LOG_IF(FATAL, externalServer_->AddService(
                      metaService_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add metaserverService error";
    LOG_IF(FATAL,
           externalServer_->AddService(copysetService_.get(),
                                       brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add copysetService error";
    LOG_IF(FATAL,
           externalServer_->AddService(raftCliService2_.get(),
                                       brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        << "add raftCliService2 error";
    LOG_IF(FATAL, externalServer_->AddService(new braft::RaftStatImpl{},
                                              brpc::SERVER_OWNS_SERVICE) != 0)
        << "add raftStatService error";

    butil::ip_t ip;
    LOG_IF(FATAL, 0 != butil::str2ip(options_.externalIp.c_str(), &ip))
        << "convert " << options_.externalIp << " to ip failed";
    butil::EndPoint listenAddr(ip, options_.externalPort);
    // start external rpc server
    LOG_IF(FATAL, externalServer_->Start(listenAddr, &option) != 0)
        << "start external brpc server error";
  }

  // try start s3compact wq
  LOG_IF(FATAL, S3CompactManager::GetInstance().Run() != 0);
  running_ = true;

  // start copyset node manager
  LOG_IF(FATAL, !copysetNodeManager_->Start())
      << "Failed to start copyset node manager";

  brpc::FLAGS_graceful_quit_on_sigterm = true;
  server_->RunUntilAskedToQuit();
}

void Metaserver::Stop() {
  if (!running_) {
    LOG(WARNING) << "Metaserver is not running";
    return;
  }

  LOG(INFO) << "MetaServer is going to quit";
  if (options_.enableExternalServer) {
    externalServer_->Stop(0);
    externalServer_->Join();
  }
  server_->Stop(0);
  server_->Join();

  PartitionCleanManager::GetInstance().Fini();

  LOG_IF(ERROR, heartbeat_.Fini() != 0);

  RecycleManager::GetInstance().Stop();

  TrashManager::GetInstance().Fini();
  LOG_IF(ERROR, !copysetNodeManager_->Stop())
      << "Failed to stop copyset node manager";

  S3CompactManager::GetInstance().Stop();
  LOG(INFO) << "MetaServer stopped success";
}

void Metaserver::InitHeartbeatOptions() {
  LOG_IF(FATAL, !conf_->GetStringValue("copyset.data_uri",
                                       &heartbeatOptions_.storeUri));
  LOG_IF(FATAL, !conf_->GetStringValue("global.ip", &heartbeatOptions_.ip));
  LOG_IF(FATAL, !conf_->GetUInt32Value("global.port", &heartbeatOptions_.port));
  LOG_IF(FATAL, !conf_->GetStringValue("mds.listen.addr",
                                       &heartbeatOptions_.mdsListenAddr));
  LOG_IF(FATAL, !conf_->GetUInt32Value("mds.heartbeat_intervalSec",
                                       &heartbeatOptions_.intervalSec));
  LOG_IF(FATAL, !conf_->GetUInt32Value("mds.heartbeat_timeoutMs",
                                       &heartbeatOptions_.timeout));
}

void Metaserver::InitHeartbeat() {
  InitHeartbeatOptions();
  heartbeatOptions_.copysetNodeManager = copysetNodeManager_;
  heartbeatOptions_.metaserverId = metadata_.id();
  heartbeatOptions_.metaserverToken = metadata_.token();
  heartbeatOptions_.fs = localFileSystem_;
  heartbeatOptions_.resourceCollector = resourceCollector_.get();
  LOG_IF(FATAL, heartbeat_.Init(heartbeatOptions_) != 0)
      << "Failed to init Heartbeat manager.";
}

void Metaserver::InitResourceCollector() {
  std::string dataRoot;
  std::string protocol = dingofs::utils::UriParser::ParseUri(
      copysetNodeOptions_.dataUri, &dataRoot);

  LOG_IF(FATAL, dataRoot.empty())
      << "Unsupported data uri: " << copysetNodeOptions_.dataUri;

  LOG_IF(FATAL, localFileSystem_->Mkdir(dataRoot) != 0)
      << "Failed to create data root: " << dataRoot << berror();

  resourceCollector_ = absl::make_unique<ResourceCollector>(
      copysetNodeOptions_.storageOptions.maxDiskQuotaBytes,
      copysetNodeOptions_.storageOptions.maxMemoryQuotaBytes,
      std::move(dataRoot));
}

void Metaserver::InitStorage() {
  StorageOptions options;

  LOG_IF(FATAL, !conf_->GetStringValue("storage.type", &options.type));
  LOG_IF(FATAL, options.type != "memory" && options.type != "rocksdb")
      << "Invalid storage type: " << options.type;
  LOG_IF(FATAL, !conf_->GetUInt64Value("storage.max_memory_quota_bytes",
                                       &options.maxMemoryQuotaBytes));
  LOG_IF(FATAL, !conf_->GetUInt64Value("storage.max_disk_quota_bytes",
                                       &options.maxDiskQuotaBytes));
  LOG_IF(FATAL, !conf_->GetBoolValue("storage.memory.compression",
                                     &options.compression));

  conf_->GetValueFatalIfFail("storage.rocksdb.perf_level",
                             &FLAGS_rocksdb_perf_level);
  conf_->GetValueFatalIfFail("storage.rocksdb.perf_slow_us",
                             &FLAGS_rocksdb_perf_slow_us);
  conf_->GetValueFatalIfFail("storage.rocksdb.perf_sampling_ratio",
                             &FLAGS_rocksdb_perf_sampling_ratio);
  LOG_IF(FATAL,
         !conf_->GetUInt64Value("storage.s3_meta_inside_inode.limit_size",
                                &options.s3MetaLimitSizeInsideInode));

  if (options.type == "rocksdb") {
    storage::ParseRocksdbOptions(conf_.get());
  }

  copysetNodeOptions_.storageOptions = std::move(options);
}

void Metaserver::InitCopysetNodeManager() {
  InitCopysetNodeOptions();

  copysetNodeManager_ = &CopysetNodeManager::GetInstance();
  LOG_IF(FATAL, !copysetNodeManager_->Init(copysetNodeOptions_))
      << "Failed to initialize CopysetNodeManager";
}

void Metaserver::InitCopysetNodeOptions() {
  LOG_IF(FATAL, !conf_->GetStringValue("global.ip", &copysetNodeOptions_.ip));
  LOG_IF(FATAL,
         !conf_->GetUInt32Value("global.port", &copysetNodeOptions_.port));

  LOG_IF(FATAL,
         copysetNodeOptions_.port <= 0 || copysetNodeOptions_.port >= 65535)
      << "Invalid server port: " << copysetNodeOptions_.port;

  LOG_IF(FATAL, !conf_->GetStringValue("copyset.data_uri",
                                       &copysetNodeOptions_.dataUri));
  LOG_IF(FATAL, !conf_->GetIntValue(
                    "copyset.election_timeout_ms",
                    &copysetNodeOptions_.raftNodeOptions.election_timeout_ms));
  LOG_IF(FATAL, !conf_->GetIntValue(
                    "copyset.snapshot_interval_s",
                    &copysetNodeOptions_.raftNodeOptions.snapshot_interval_s));
  LOG_IF(FATAL, !conf_->GetIntValue(
                    "copyset.catchup_margin",
                    &copysetNodeOptions_.raftNodeOptions.catchup_margin));
  LOG_IF(FATAL,
         !conf_->GetStringValue("copyset.raft_log_uri",
                                &copysetNodeOptions_.raftNodeOptions.log_uri));
  LOG_IF(FATAL, !conf_->GetStringValue(
                    "copyset.raft_meta_uri",
                    &copysetNodeOptions_.raftNodeOptions.raft_meta_uri));
  LOG_IF(FATAL, !conf_->GetStringValue(
                    "copyset.raft_snapshot_uri",
                    &copysetNodeOptions_.raftNodeOptions.snapshot_uri));
  LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.load_concurrency",
                                       &copysetNodeOptions_.loadConcurrency));
  LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.check_retrytimes",
                                       &copysetNodeOptions_.checkRetryTimes));
  LOG_IF(FATAL, !conf_->GetUInt32Value("copyset.finishload_margin",
                                       &copysetNodeOptions_.finishLoadMargin));
  LOG_IF(FATAL, !conf_->GetUInt32Value(
                    "copyset.check_loadmargin_interval_ms",
                    &copysetNodeOptions_.checkLoadMarginIntervalMs));

  LOG_IF(FATAL, !conf_->GetUInt32Value(
                    "applyqueue.worker_count",
                    &copysetNodeOptions_.applyQueueOption.workerCount));
  LOG_IF(FATAL, !conf_->GetUInt32Value(
                    "applyqueue.queue_depth",
                    &copysetNodeOptions_.applyQueueOption.queueDepth));

  LOG_IF(FATAL,
         !conf_->GetStringValue("copyset.trash.uri",
                                &copysetNodeOptions_.trashOptions.trashUri));
  LOG_IF(FATAL, !conf_->GetUInt32Value(
                    "copyset.trash.expired_aftersec",
                    &copysetNodeOptions_.trashOptions.expiredAfterSec));
  LOG_IF(FATAL, !conf_->GetUInt32Value(
                    "copyset.trash.scan_periodsec",
                    &copysetNodeOptions_.trashOptions.scanPeriodSec));

  CHECK(localFileSystem_);
  copysetNodeOptions_.localFileSystem = localFileSystem_.get();
}

void Metaserver::InitInflightThrottle() {
  uint64_t maxInflight = 0;
  LOG_IF(FATAL,
         !conf_->GetUInt64Value("service.max_inflight_request", &maxInflight));

  inflightThrottle_ = absl::make_unique<InflightThrottle>(maxInflight);
}

struct TakeValueFromConfIfCmdNotSet {
  template <typename T>
  void operator()(const std::shared_ptr<Configuration>& conf,
                  const std::string& cmdName, const std::string& confName,
                  T* value) {
    using ::google::CommandLineFlagInfo;
    using ::google::GetCommandLineFlagInfo;

    CommandLineFlagInfo info;
    if (GetCommandLineFlagInfo(cmdName.c_str(), &info) && info.is_default) {
      conf->GetValueFatalIfFail(confName, value);
    }
  }
};

void Metaserver::InitBRaftFlags(const std::shared_ptr<Configuration>& conf) {
  TakeValueFromConfIfCmdNotSet dummy;
  dummy(conf, "raft_sync", "braft.raft_sync", &braft::FLAGS_raft_sync);
  dummy(conf, "raft_sync_meta", "braft.raft_sync_meta",
        &braft::FLAGS_raft_sync_meta);
  dummy(conf, "raft_sync_segments", "braft.raft_sync_segments",
        &braft::FLAGS_raft_sync_segments);
  dummy(conf, "raft_use_fsync_rather_than_fdatasync",
        "braft.raft_use_fsync_rather_than_fdatasync",
        &braft::FLAGS_raft_use_fsync_rather_than_fdatasync);
  dummy(conf, "raft_max_install_snapshot_tasks_num",
        "braft.raft_max_install_snapshot_tasks_num",
        &braft::FLAGS_raft_max_install_snapshot_tasks_num);
}

}  // namespace metaserver
}  // namespace dingofs
