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

/*************************************************************************
> File Name: mock_s3infocache.h
> Author:
> Created Time: Thu 4 Nov 2021
 ************************************************************************/

#ifndef DINGOFS_TEST_METASERVER_S3COMPACT_MOCK_S3INFOCACHE_H_
#define DINGOFS_TEST_METASERVER_S3COMPACT_MOCK_S3INFOCACHE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "dingofs/common.pb.h"
#include "metaserver/compaction/fs_info_cache.h"

using ::testing::Invoke;
using ::testing::Return;

namespace dingofs {
namespace metaserver {

class MockS3InfoCache : public S3InfoCache {
 public:
  MockS3InfoCache(uint64_t capacity, std::vector<std::string> mdsAddrs,
                  butil::EndPoint metaserverAddr)
      : S3InfoCache(capacity, mdsAddrs, metaserverAddr) {}
  ~MockS3InfoCache() override = default;
  MOCK_METHOD2(RequestS3Info, S3InfoCache::S3InfoCache::RequestStatusCode(
                                  uint64_t, pb::common::S3Info*));
  MOCK_METHOD2(GetS3Info, int(uint64_t, pb::common::S3Info*));
  MOCK_METHOD1(InvalidateS3Info, void(uint64_t));
};

}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_TEST_METASERVER_S3COMPACT_MOCK_S3INFOCACHE_H_
