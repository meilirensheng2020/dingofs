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
 * Created Date: Wed Mar 23 2022
 * Author: huyao
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "client/vfs_legacy/mock_client_s3_cache_manager.h"
#include "client/vfs_legacy/s3/client_s3_adaptor.h"
#include "client/vfs_legacy/s3/client_s3_cache_manager.h"
#include "options/client/vfs_legacy/vfs_legacy_option.h"

namespace dingofs {
namespace client {

class DataCacheTest : public testing::Test {
 public:
  DataCacheTest() = default;

  ~DataCacheTest() override = default;

  void SetUp() override {
    S3ClientAdaptorOption option;
    option.blockSize = 1 * 1024 * 1024;
    option.chunkSize = 4 * 1024 * 1024;
    option.baseSleepUs = 500;
    option.pageSize = 64 * 1024;
    option.intervalMs = 5000 * 1000;
    option.flushIntervalSec = 5000;
    option.readCacheMaxByte = 104857600;
    option.readCacheThreads = 5;
    s3ClientAdaptor_ = new S3ClientAdaptorImpl();
    auto fs_cache_manager = std::make_shared<FsCacheManager>(
        s3ClientAdaptor_, option.readCacheMaxByte, option.writeCacheMaxByte,
        option.readCacheThreads, nullptr);
    s3ClientAdaptor_->Init(option, nullptr, nullptr, nullptr, fs_cache_manager,
                           nullptr, nullptr, nullptr);
    mockChunkCacheManager_ = std::make_shared<MockChunkCacheManager>();
    uint64_t offset = 512 * 1024;
    uint64_t len = 1024 * 1024;
    char* buf = new char[len];
    dataCache_ = std::make_shared<DataCache>(
        s3ClientAdaptor_, mockChunkCacheManager_, offset, len, buf, nullptr);
    delete[] buf;
  }
  void TearDown() override {}

 protected:
  S3ClientAdaptorImpl* s3ClientAdaptor_;
  std::shared_ptr<DataCache> dataCache_;
  std::shared_ptr<MockChunkCacheManager> mockChunkCacheManager_;
};

TEST_F(DataCacheTest, test_write1) {
  uint64_t offset = 0;
  uint64_t len = 1024 * 1024;
  char* buf = new char[len];
  std::vector<DataCachePtr> mergeDataCacheVer;
  dataCache_->Write(offset, len, buf, mergeDataCacheVer);
  ASSERT_EQ(0, dataCache_->GetChunkPos());
  ASSERT_EQ(1536 * 1024, dataCache_->GetLen());
  delete[] buf;
}

TEST_F(DataCacheTest, test_write2) {
  uint64_t offset = 0;
  uint64_t len = 2 * 1024 * 1024;
  char* buf = new char[len];
  std::vector<DataCachePtr> mergeDataCacheVer;
  dataCache_->Write(offset, len, buf, mergeDataCacheVer);
  ASSERT_EQ(0, dataCache_->GetChunkPos());
  ASSERT_EQ(2048 * 1024, dataCache_->GetLen());
  delete[] buf;
}

TEST_F(DataCacheTest, test_write3) {
  uint64_t offset = 0;
  uint64_t len = 2 * 1024 * 1024;
  char* buf = new char[len];
  std::vector<DataCachePtr> mergeDataCacheVer;
  uint64_t offset1 = len;
  auto dataCache = std::make_shared<DataCache>(
      s3ClientAdaptor_, mockChunkCacheManager_, offset1, len, buf, nullptr);
  mergeDataCacheVer.push_back(dataCache);
  dataCache_->Write(offset, len, buf, mergeDataCacheVer);
  ASSERT_EQ(0, dataCache_->GetChunkPos());
  ASSERT_EQ(4096 * 1024, dataCache_->GetLen());
  delete[] buf;
}

TEST_F(DataCacheTest, test_write4) {
  uint64_t offset = 512 * 1204;
  uint64_t len = 512 * 1024;
  char* buf = new char[len];
  std::vector<DataCachePtr> mergeDataCacheVer;
  dataCache_->Write(offset, len, buf, mergeDataCacheVer);
  ASSERT_EQ(512 * 1024, dataCache_->GetChunkPos());
  ASSERT_EQ(1024 * 1024, dataCache_->GetLen());
  delete[] buf;
}

TEST_F(DataCacheTest, test_write5) {
  uint64_t offset = 1024 * 1024;
  uint64_t len = 1024 * 1024;
  char* buf = new char[len];
  std::vector<DataCachePtr> mergeDataCacheVer;
  dataCache_->Write(offset, len, buf, mergeDataCacheVer);
  ASSERT_EQ(512 * 1024, dataCache_->GetChunkPos());
  ASSERT_EQ(1536 * 1024, dataCache_->GetLen());
  delete[] buf;
}

TEST_F(DataCacheTest, test_write6) {
  uint64_t offset = 1024 * 1024;
  uint64_t len = 1024 * 1024;
  char* buf = new char[len];
  std::vector<DataCachePtr> mergeDataCacheVer;
  uint64_t offset1 = offset + len;
  auto data_cache = std::make_shared<DataCache>(
      s3ClientAdaptor_, mockChunkCacheManager_, offset1, len, buf, nullptr);
  mergeDataCacheVer.push_back(data_cache);
  dataCache_->Write(offset, len, buf, mergeDataCacheVer);
  ASSERT_EQ(512 * 1024, dataCache_->GetChunkPos());
  ASSERT_EQ(2560 * 1024, dataCache_->GetLen());
  delete[] buf;
}

TEST_F(DataCacheTest, test_truncate1) {
  uint64_t size = 0;
  dataCache_->Truncate(size);
  ASSERT_EQ(0, dataCache_->GetLen());
}

TEST_F(DataCacheTest, test_truncate2) {
  uint64_t size = 512 * 1024;
  dataCache_->Truncate(size);
  ASSERT_EQ(512 * 1024, dataCache_->GetLen());
}

TEST_F(DataCacheTest, test_truncate3) {
  uint64_t size = 2;
  dataCache_->Truncate(size);
  ASSERT_EQ(2, dataCache_->GetLen());
}

}  // namespace client
}  // namespace dingofs
