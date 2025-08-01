// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs_legacy/filesystem/fs_push_metric_manager.h"

#include "glog/logging.h"
#include "metrics/blockaccess/block_accesser.h"
#include "metrics/client/client.h"
#include "options/client/vfs_legacy/vfs_legacy_dynamic_config.h"

namespace dingofs {
namespace client {
namespace filesystem {

using ::dingofs::metrics::client::VFSRWMetric;
using ::dingofs::pb::mds::FsStatsData;
using ::dingofs::pb::mds::FSStatusCode;
using metrics::blockaccess::BlockMetric;

USING_FLAG(push_metric_interval_millsecond)

void FsPushMetricManager::Start() {
  if (running_.load()) {
    return;
  }

  executor_->Schedule([this] { PushClientMetrics(); },
                      FLAGS_push_metric_interval_millsecond);

  running_.store(true);
}

void FsPushMetricManager::Stop() {
  if (!running_.load()) {
    return;
  }

  DoPushClientMetrics();

  running_.store(false);
}

bool FsPushMetricManager::IsRunning() const { return running_.load(); }

void FsPushMetricManager::PushClientMetrics() {
  if (!IsRunning()) {
    LOG(WARNING) << "PushClientMetrics not running";
    return;
  }

  DoPushClientMetrics();

  executor_->Schedule([this] { PushClientMetrics(); },
                      FLAGS_push_metric_interval_millsecond);
}

void FsPushMetricManager::DoPushClientMetrics() {
  CHECK_NOTNULL(mds_client_);

  FsStatsData current_client_metrics = GetClientMetrics();
  FsStatsData delta_client_metrics =
      current_client_metrics - last_client_metrics_;
  FSStatusCode rc = mds_client_->SetFsStats(fsname_, delta_client_metrics);

  if (rc != FSStatusCode::OK) {
    LOG(WARNING) << "PushClientMetrics failed, fs_name: " << fsname_
                 << ", rc: " << rc << ", delta metrics data:["
                 << delta_client_metrics.ShortDebugString() << "]";
  } else {
    VLOG(9) << "PushClientMetrics success, fs_name: " << fsname_
            << ",delta metrics data:["
            << delta_client_metrics.ShortDebugString() << "]";
    last_client_metrics_ = current_client_metrics;
  }
}

FsStatsData FsPushMetricManager::GetClientMetrics() {
  FsStatsData client_metrics;

  // filesystem read metrics
  client_metrics.set_readbytes(
      VFSRWMetric::GetInstance().read.bps.count.get_value());
  client_metrics.set_readqps(
      VFSRWMetric::GetInstance().read.qps.count.get_value());
  client_metrics.set_writebytes(
      VFSRWMetric::GetInstance().write.bps.count.get_value());
  client_metrics.set_writeqps(
      VFSRWMetric::GetInstance().write.qps.count.get_value());
  // s3 write metrics
  client_metrics.set_s3readbytes(
      BlockMetric::GetInstance().read_block.bps.count.get_value());
  client_metrics.set_s3readqps(
      BlockMetric::GetInstance().read_block.qps.count.get_value());
  client_metrics.set_s3writebytes(
      BlockMetric::GetInstance().write_block.bps.count.get_value());
  client_metrics.set_s3writeqps(
      BlockMetric::GetInstance().write_block.qps.count.get_value());

  return client_metrics;
}

}  // namespace filesystem
}  // namespace client
}  // namespace dingofs