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
 * @Project: dingo
 * @Date: 2021-06-10 10:04:21
 * @Author: chenwei
 */
#include "mds/fs_storage.h"

#include <gmock/gmock.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "dingofs/common.pb.h"

using ::google::protobuf::util::MessageDifferencer;

namespace dingofs {
namespace mds {

bool operator==(const FsInfoWrapper& lhs, const FsInfoWrapper& rhs) {
  return MessageDifferencer::Equals(lhs.ProtoFsInfo(), rhs.ProtoFsInfo());
}

class FSStorageTest : public ::testing::Test {
 protected:
  void SetUp() override { }

  void TearDown() override { }
};

TEST_F(FSStorageTest, test1) {
  MemoryFsStorage storage;
  uint32_t fsId = 1;
  uint64_t rootInodeId = 1;
  uint64_t blockSize = 4096;


  pb::mds::CreateFsRequest req;
  req.set_fsname("name1");
  req.set_block_size(blockSize);
  req.set_chunk_size(16384);
  req.set_owner("test");
  req.set_capacity((uint64_t)100 * 1024 * 1024 * 1024);

  auto* storage_info = req.mutable_storage_info();
  storage_info->set_type(pb::common::StorageType::TYPE_S3);
  auto* s3_info = storage_info->mutable_s3_info();
  s3_info->set_ak("a");
  s3_info->set_sk("b");
  s3_info->set_endpoint("http://127.0.1:9000");
  s3_info->set_bucketname("test");

  FsInfoWrapper fs1 =
      FsInfoWrapper(&req, fsId, pb::mds::INSERT_ROOT_INODE_ERROR);
  // test insert
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Insert(fs1));
  ASSERT_EQ(pb::mds::FSStatusCode::FS_EXIST, storage.Insert(fs1));

  // test get
  FsInfoWrapper fs2;
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Get(fs1.GetFsId(), &fs2));
  ASSERT_EQ(fs1.GetFsName(), fs2.GetFsName());
  ASSERT_TRUE(fs1 == fs2);
  ASSERT_EQ(pb::mds::FSStatusCode::NOT_FOUND,
            storage.Get(fs1.GetFsId() + 1, &fs2));
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Get(fs1.GetFsName(), &fs2));
  ASSERT_EQ(fs2.GetFsName(), fs1.GetFsName());
  ASSERT_EQ(pb::mds::FSStatusCode::NOT_FOUND,
            storage.Get(fs1.GetFsName() + "1", &fs2));
  ASSERT_TRUE(storage.Exist(fs1.GetFsId()));
  ASSERT_FALSE(storage.Exist(fs1.GetFsId() + 1));
  ASSERT_TRUE(storage.Exist(fs1.GetFsName()));
  ASSERT_FALSE(storage.Exist(fs1.GetFsName() + "1"));

  // test update
  fs1.SetStatus(pb::mds::FsStatus::INITED);
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Update(fs1));
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Get(fs1.GetFsId(), &fs2));
  ASSERT_EQ(fs2.GetStatus(), pb::mds::FsStatus::INITED);
  req.set_fsname("name3");
  FsInfoWrapper fs3 = FsInfoWrapper(&req, fsId, rootInodeId);

  LOG(INFO) << "NAME " << fs3.GetFsName();
  ASSERT_EQ(pb::mds::FSStatusCode::NOT_FOUND, storage.Update(fs3));

  req.set_fsname("name1");
  FsInfoWrapper fs4 = FsInfoWrapper(&req, fsId + 1, rootInodeId);
  ASSERT_EQ(pb::mds::FSStatusCode::FS_ID_MISMATCH, storage.Update(fs4));

  // test rename
  FsInfoWrapper fs5 = fs1;
  fs5.SetFsName("name5");
  fs5.SetStatus(pb::mds::FsStatus::DELETING);
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Rename(fs1, fs5));
  FsInfoWrapper fs6;
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Get(fs1.GetFsId(), &fs6));
  ASSERT_EQ(fs6.GetStatus(), pb::mds::FsStatus::DELETING);
  ASSERT_EQ(fs6.GetFsName(), "name5");

  // test delete
  ASSERT_EQ(pb::mds::FSStatusCode::NOT_FOUND, storage.Delete(fs1.GetFsName()));
  ASSERT_EQ(pb::mds::FSStatusCode::OK, storage.Delete(fs5.GetFsName()));
  ASSERT_EQ(pb::mds::FSStatusCode::NOT_FOUND, storage.Delete(fs5.GetFsName()));
}
}  // namespace mds
}  // namespace dingofs
