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
 * Project: dingofs
 * Created Date: 2021/11/1
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
#define DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_

#include <bvar/bvar.h>

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "dingofs/metaserver.pb.h"
#include "mds/common/mds_define.h"
#include "mds/topology/topology.h"
#include "utils/interruptible_sleeper.h"

namespace dingofs {
namespace mds {
namespace topology {

using pb::metaserver::FsFileType;
using pb::metaserver::FsFileType_IsValid;
using pb::metaserver::FsFileType_MAX;
using pb::metaserver::FsFileType_MIN;
using pb::metaserver::FsFileType_Name;

using utils::InterruptibleSleeper;
struct MetaServerMetric {
  const std::string kTopologyMetaServerMetricPrefix =
      "topology_metric_metaserver_id_";
  // scatterWidth
  bvar::Status<uint32_t> scatterWidth;
  // copyset numbers
  bvar::Status<uint32_t> copysetNum;
  // leader numbers
  bvar::Status<uint32_t> leaderNum;
  // disk threshold, Byte
  bvar::Status<uint64_t> diskThreshold;
  // disk utilization, Byte
  bvar::Status<uint64_t> diskUsed;
  // disk copyset min required, Byte
  bvar::Status<uint64_t> diskMinRequire;
  // memory threshold, Byte
  bvar::Status<uint64_t> memoryThreshold;
  // memory utilization, Byte
  bvar::Status<uint64_t> memoryUsed;
  // memory copyset min required, Byte
  bvar::Status<uint64_t> memoryMinRequire;
  // partition numbers
  bvar::Status<uint32_t> partitionNum;

  explicit MetaServerMetric(MetaServerIdType msId)
      : scatterWidth(kTopologyMetaServerMetricPrefix,
                     std::to_string(msId) + "_scatterwidth", 0),
        copysetNum(kTopologyMetaServerMetricPrefix,
                   std::to_string(msId) + "_copyset_num", 0),
        leaderNum(kTopologyMetaServerMetricPrefix,
                  std::to_string(msId) + "_leader_num", 0),
        diskThreshold(kTopologyMetaServerMetricPrefix,
                      std::to_string(msId) + "_disk_threshold", 0),
        diskUsed(kTopologyMetaServerMetricPrefix,
                 std::to_string(msId) + "_disk_used", 0),
        diskMinRequire(kTopologyMetaServerMetricPrefix,
                       std::to_string(msId) + "_disk_min_require", 0),
        memoryThreshold(kTopologyMetaServerMetricPrefix,
                        std::to_string(msId) + "_memory_threshold", 0),
        memoryUsed(kTopologyMetaServerMetricPrefix,
                   std::to_string(msId) + "_memory_used", 0),
        memoryMinRequire(kTopologyMetaServerMetricPrefix,
                         std::to_string(msId) + "_memory_min_require", 0),
        partitionNum(kTopologyMetaServerMetricPrefix,
                     std::to_string(msId) + "_partition_num", 0) {}
};

using MetaServerMetricPtr = std::unique_ptr<MetaServerMetric>;

struct PoolMetric {
  const std::string kTopologyPoolMetricPrefix = "topology_metric_pool_";
  bvar::Status<uint32_t> metaServerNum;
  bvar::Status<uint32_t> copysetNum;
  bvar::Status<uint64_t> diskThreshold;
  bvar::Status<uint64_t> diskUsed;
  bvar::Status<uint64_t> memoryThreshold;
  bvar::Status<uint64_t> memoryUsed;
  bvar::Status<uint64_t> inodeNum;
  bvar::Status<uint64_t> dentryNum;
  bvar::Status<uint64_t> partitionNum;

  explicit PoolMetric(const std::string& poolName)
      : metaServerNum(kTopologyPoolMetricPrefix, poolName + "_metaserver_num",
                      0),
        copysetNum(kTopologyPoolMetricPrefix, poolName + "_copyset_num", 0),

        diskThreshold(kTopologyPoolMetricPrefix, poolName + "_disk_threshold",
                      0),
        diskUsed(kTopologyPoolMetricPrefix, poolName + "_disk_used", 0),
        memoryThreshold(kTopologyPoolMetricPrefix,
                        poolName + "_memory_threshold", 0),
        memoryUsed(kTopologyPoolMetricPrefix, poolName + "_memory_used", 0),
        inodeNum(kTopologyPoolMetricPrefix, poolName + "_inode_num", 0),
        dentryNum(kTopologyPoolMetricPrefix, poolName + "_dentry_num", 0),
        partitionNum(kTopologyPoolMetricPrefix, poolName + "_partition_num",
                     0) {}
};
using PoolMetricPtr = std::unique_ptr<PoolMetric>;

extern std::map<PoolIdType, PoolMetricPtr> gPoolMetrics;
extern std::map<MetaServerIdType, MetaServerMetricPtr> gMetaServerMetrics;

class TopologyMetricService {
 public:
  struct MetaServerMetricInfo {
    uint32_t scatterWidth;
    uint32_t copysetNum;
    uint32_t leaderNum;
    uint32_t partitionNum;

    MetaServerMetricInfo()
        : scatterWidth(0), copysetNum(0), leaderNum(0), partitionNum(0) {}
  };

 public:
  explicit TopologyMetricService(std::shared_ptr<Topology> topo)
      : topo_(topo), isStop_(true) {}
  ~TopologyMetricService() { Stop(); }

  void Init(const TopologyOption& option);

  void Run();

  void Stop();

  void UpdateTopologyMetrics();

  /**
   * @brief calculate metric data of metaserver
   *
   * @param copysets CopySetInfo list
   * @param[out] msMetricInfoMap metric data
   */
  void CalcMetaServerMetrics(
      const std::vector<CopySetInfo>& copysets,
      std::map<MetaServerIdType, MetaServerMetricInfo>* msMetricInfoMap);

 private:
  /**
   * @brief backend function that executes UpdateTopologyMetrics regularly
   */
  void BackEndFunc();

 private:
  /**
   * @brief topology module
   */
  std::shared_ptr<Topology> topo_;

  /**
   * @brief backend thread
   */
  dingofs::utils::Thread backEndThread_;
  /**
   * @brief flag marking out stopping the backend thread or not
   */
  dingofs::utils::Atomic<bool> isStop_;

  InterruptibleSleeper sleeper_;

  /**
   * @brief topology options
   */
  TopologyOption option_;
};

struct FsMetric {
 public:
  const std::string kTopologyFsMetricPrefix = "topology_Fs_id_";
  // inode number

  bvar::Status<uint64_t> inodeNum_;
  using statusPtr = std::shared_ptr<bvar::Status<uint64_t>>;
  std::unordered_map<FsFileType, statusPtr> fileType2InodeNum_;

  explicit FsMetric(FsIdType fsId)
      : inodeNum_(kTopologyFsMetricPrefix, std::to_string(fsId) + "_inode_num",
                  0) {
    for (int i = FsFileType_MIN; i <= FsFileType_MAX; ++i) {
      if (FsFileType_IsValid(i)) {
        auto inodeNumTmpPtr = std::make_shared<bvar::Status<uint64_t>>(
            kTopologyFsMetricPrefix,
            std::to_string(fsId) + "_" +
                FsFileType_Name(static_cast<FsFileType>(i)) + "_inode_num",
            0);

        fileType2InodeNum_.emplace(static_cast<FsFileType>(i),
                                   std::move(inodeNumTmpPtr));
      }
    }
  }
};

}  // namespace topology
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_TOPOLOGY_TOPOLOGY_METRIC_H_
