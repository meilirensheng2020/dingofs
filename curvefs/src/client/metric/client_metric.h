/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"){}
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
 * Project: curve
 * Created Date: Thur Oct Jun 28 2021
 * Author: lixiaocui
 */

#ifndef CURVEFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_
#define CURVEFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_

#include <bvar/bvar.h>

#include <cstdint>
#include <string>

#include "src/common/string_util.h"

namespace curvefs {
namespace client {
namespace metric {

// metric stats per second
struct PerSecondMetric {
  bvar::Adder<uint64_t> count;                   // total count
  bvar::PerSecond<bvar::Adder<uint64_t>> value;  // average count persecond
  PerSecondMetric(const std::string& prefix, const std::string& name)
      : count(prefix, name + "_total_count"), value(prefix, name, &count, 1) {}
};

// interface metric statistics
struct InterfaceMetric {
  PerSecondMetric qps;            // processed per second
  PerSecondMetric eps;            // error request per second
  PerSecondMetric bps;            // throughput with byte per second
  bvar::LatencyRecorder latency;  // latency

  InterfaceMetric(const std::string& prefix, const std::string& name)
      : qps(prefix, name + "_qps"),
        eps(prefix, name + "_eps"),
        bps(prefix, name + "_bps"),
        latency(prefix, name + "_lat", 1) {}
};

struct MDSClientMetric {
  static const std::string prefix;

  InterfaceMetric mountFs;
  InterfaceMetric umountFs;
  InterfaceMetric getFsInfo;
  InterfaceMetric getMetaServerInfo;
  InterfaceMetric getMetaServerListInCopysets;
  InterfaceMetric createPartition;
  InterfaceMetric getCopysetOfPartitions;
  InterfaceMetric listPartition;
  InterfaceMetric allocS3ChunkId;
  InterfaceMetric refreshSession;
  InterfaceMetric getLatestTxId;
  InterfaceMetric commitTx;
  InterfaceMetric allocOrGetMemcacheCluster;
  // all
  InterfaceMetric getAllOperation;

  MDSClientMetric()
      : mountFs(prefix, "mountFs"),
        umountFs(prefix, "umountFs"),
        getFsInfo(prefix, "getFsInfo"),
        getMetaServerInfo(prefix, "getMetaServerInfo"),
        getMetaServerListInCopysets(prefix, "getMetaServerListInCopysets"),
        createPartition(prefix, "createPartition"),
        getCopysetOfPartitions(prefix, "getCopysetOfPartitions"),
        listPartition(prefix, "listPartition"),
        allocS3ChunkId(prefix, "allocS3ChunkId"),
        refreshSession(prefix, "refreshSession"),
        getLatestTxId(prefix, "getLatestTxId"),
        commitTx(prefix, "commitTx"),
        allocOrGetMemcacheCluster(prefix, "allocOrGetMemcacheCluster"),
        getAllOperation(prefix, "getAllopt") {}
};

struct MetaServerClientMetric {
  static const std::string prefix;

  // dentry
  InterfaceMetric getDentry;
  InterfaceMetric listDentry;
  InterfaceMetric createDentry;
  InterfaceMetric deleteDentry;

  // inode
  InterfaceMetric getInode;
  InterfaceMetric batchGetInodeAttr;
  InterfaceMetric batchGetXattr;
  InterfaceMetric createInode;
  InterfaceMetric updateInode;
  InterfaceMetric deleteInode;
  InterfaceMetric appendS3ChunkInfo;

  // txn
  InterfaceMetric prepareRenameTx;

  // volume extent
  InterfaceMetric updateVolumeExtent;
  InterfaceMetric getVolumeExtent;

  // write operation
  InterfaceMetric getTxnOperation;
  // all
  InterfaceMetric getAllOperation;

  MetaServerClientMetric()
      : getDentry(prefix, "getDentry"),
        listDentry(prefix, "listDentry"),
        createDentry(prefix, "createDentry"),
        deleteDentry(prefix, "deleteDentry"),
        getInode(prefix, "getInode"),
        batchGetInodeAttr(prefix, "batchGetInodeAttr"),
        batchGetXattr(prefix, "batchGetXattr"),
        createInode(prefix, "createInode"),
        updateInode(prefix, "updateInode"),
        deleteInode(prefix, "deleteInode"),
        appendS3ChunkInfo(prefix, "appendS3ChunkInfo"),
        prepareRenameTx(prefix, "prepareRenameTx"),
        updateVolumeExtent(prefix, "updateVolumeExtent"),
        getVolumeExtent(prefix, "getVolumeExtent"),
        getTxnOperation(prefix, "getTxnopt"),
        getAllOperation(prefix, "getAllopt") {}
};

struct OpMetric {
  bvar::LatencyRecorder latency;
  bvar::Adder<int64_t> inflightOpNum;
  bvar::Adder<uint64_t> ecount;

  explicit OpMetric(const std::string& prefix, const std::string& name)
      : latency(prefix, name + "_lat", 1),
        inflightOpNum(prefix, name + "_inflight_num"),
        ecount(prefix, name + "_error_num") {}
};

struct ClientOpMetric {
  static const std::string prefix;

  OpMetric opLookup;
  OpMetric opOpen;
  OpMetric opCreate;
  OpMetric opMkNod;
  OpMetric opMkDir;
  OpMetric opLink;
  OpMetric opUnlink;
  OpMetric opRmDir;
  OpMetric opOpenDir;
  OpMetric opReleaseDir;
  OpMetric opReadDir;
  OpMetric opRename;
  OpMetric opGetAttr;
  OpMetric opSetAttr;
  OpMetric opGetXattr;
  OpMetric opListXattr;
  OpMetric opSymlink;
  OpMetric opReadLink;
  OpMetric opRelease;
  OpMetric opFsync;
  OpMetric opFlush;
  OpMetric opRead;
  OpMetric opWrite;
  OpMetric opAll;

  ClientOpMetric()
      : opLookup(prefix, "opLookup"),
        opOpen(prefix, "opOpen"),
        opCreate(prefix, "opCreate"),
        opMkNod(prefix, "opMknod"),
        opMkDir(prefix, "opMkdir"),
        opLink(prefix, "opLink"),
        opUnlink(prefix, "opUnlink"),
        opRmDir(prefix, "opRmdir"),
        opOpenDir(prefix, "opOpendir"),
        opReleaseDir(prefix, "opReleasedir"),
        opReadDir(prefix, "opReaddir"),
        opRename(prefix, "opRename"),
        opGetAttr(prefix, "opGetattr"),
        opSetAttr(prefix, "opSetattr"),
        opGetXattr(prefix, "opGetxattr"),
        opListXattr(prefix, "opListxattr"),
        opSymlink(prefix, "opSymlink"),
        opReadLink(prefix, "opReadlink"),
        opRelease(prefix, "opRelease"),
        opFsync(prefix, "opFsync"),
        opFlush(prefix, "opFlush"),
        opRead(prefix, "opRead"),
        opWrite(prefix, "opWrite"),
        opAll(prefix, "opAll") {}
};

struct S3MultiManagerMetric {
  static const std::string prefix;

  bvar::Adder<int64_t> fileManagerNum;
  bvar::Adder<int64_t> chunkManagerNum;
  bvar::Adder<int64_t> writeDataCacheNum;
  bvar::Adder<int64_t> writeDataCacheByte;
  bvar::Adder<int64_t> readDataCacheNum;
  bvar::Adder<int64_t> readDataCacheByte;

  S3MultiManagerMetric() {
    fileManagerNum.expose_as(prefix, "file_manager_num");
    chunkManagerNum.expose_as(prefix, "chunk_manager_num");
    writeDataCacheNum.expose_as(prefix, "write_data_cache_num");
    writeDataCacheByte.expose_as(prefix, "write_data_cache_byte");
    readDataCacheNum.expose_as(prefix, "read_data_cache_num");
    readDataCacheByte.expose_as(prefix, "read_data_cache_byte");
  }
};

struct FSMetric {
  static const std::string prefix;

  std::string fsName;

  InterfaceMetric userWrite;
  InterfaceMetric userRead;
  bvar::Status<uint32_t> userWriteIoSize;  // last write io size
  bvar::Status<uint32_t> userReadIoSize;   // last read io size

  explicit FSMetric(const std::string& name = "")
      : userWrite(prefix, "_userWrite"),
        userRead(prefix, "_userRead"),
        userWriteIoSize(prefix, "_userWriteIoSizeLast", 0),
        userReadIoSize(prefix, "_userReadIoSizeLast", 0) {
    (void)name;
  }
};

struct S3Metric {
  static const std::string prefix;

  std::string fsName;
  InterfaceMetric adaptorWrite;
  InterfaceMetric adaptorRead;
  InterfaceMetric adaptorWriteS3;
  InterfaceMetric adaptorReadS3;
  InterfaceMetric adaptorWriteDiskCache;
  InterfaceMetric adaptorReadDiskCache;
  bvar::Status<uint32_t> writeSize;
  bvar::Status<uint32_t> readSize;
  // bvar::PerSecondEx<bvar::Adder<uint32_t>, 1> writeSizeSecond;
  // bvar::PerSecondEx<bvar::Adder<uint32_t>, 1> readSizeSecond;

  explicit S3Metric(const std::string& name = "")
      : adaptorWrite(prefix, "_adaptor_write"),
        adaptorRead(prefix, "_adaptor_read"),
        adaptorWriteS3(prefix, "_adaptor_write_s3"),
        adaptorReadS3(prefix, "_adaptor_read_s3"),
        adaptorWriteDiskCache(prefix, "_adaptor_write_diskcache"),
        adaptorReadDiskCache(prefix, "_adaptor_read_diskcache"),
        writeSize(prefix, "_adaptor_write_size_last", 0),
        readSize(prefix, "_adaptor_read_size_last", 0) {
    (void)name;
  }
};

struct DiskCacheMetric {
  static const std::string prefix;

  std::string fsName;
  // move to S3Metric adaptorWriteS3
  // InterfaceMetric writeS3;
  bvar::Status<uint64_t> diskUsedBytes;
  bvar::PerSecondEx<bvar::Adder<uint32_t>, 1> diskcacheWriteSizeSecond;
  bvar::PerSecondEx<bvar::Adder<uint32_t>, 1> diskcacheReadSizeSecond;

  explicit DiskCacheMetric(const std::string& name = "")
      :  // writeS3(prefix, fsName + "_write_s3"),
        diskUsedBytes(prefix, "_diskcache_usedbytes", 0),
        diskcacheWriteSizeSecond(prefix + "_write_size_second"),
        diskcacheReadSizeSecond(prefix + "_read_size_second") {
    (void)name;
  }
};

struct KVClientMetric {
  static const std::string prefix;
  InterfaceMetric kvClientGet;
  InterfaceMetric kvClientSet;

  KVClientMetric() : kvClientGet(prefix, "get"), kvClientSet(prefix, "set") {}
};

struct S3ChunkInfoMetric {
  static const std::string prefix;

  bvar::Adder<int64_t> s3ChunkInfoSize;

  S3ChunkInfoMetric() : s3ChunkInfoSize(prefix, "size") {}
};

struct WarmupManagerS3Metric {
  static const std::string prefix;

  InterfaceMetric warmupS3Cached;
  bvar::Adder<uint64_t> warmupS3CacheSize;

  WarmupManagerS3Metric()
      : warmupS3Cached(prefix, "s3_cached"),
        warmupS3CacheSize(prefix, "s3_cache_size") {}
};

}  // namespace metric
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_METRIC_CLIENT_METRIC_H_
