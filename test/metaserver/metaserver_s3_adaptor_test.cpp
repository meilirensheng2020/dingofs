/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 2021-8-13
 * Author: chengyi
 */

#include "metaserver/metaserver_s3_adaptor_test.h"

using ::dingofs::pb::common::PartitionInfo;
using ::dingofs::pb::metaserver::Dentry;
using ::dingofs::pb::metaserver::FsFileType;
using ::dingofs::pb::metaserver::Inode;
using ::dingofs::pb::metaserver::ManageInodeType;
using ::dingofs::pb::metaserver::MetaStatusCode;

namespace dingofs {
namespace metaserver {

class MetaserverS3AdaptorTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(0, server_.AddService(&mockMetaServerService_,
                                    brpc::SERVER_DOESNT_OWN_SERVICE));
    ASSERT_EQ(0, server_.Start(addr_.c_str(), nullptr));

    S3ClientAdaptorOption option;
    option.blockSize = 1 * 1024 * 1024;
    option.chunkSize = 4 * 1024 * 1024;
    option.batchSize = 5;
    option.objectPrefix = 0;
    option.enableDeleteObjects = false;
    mockMetaserverS3Client_ = new MockS3Client();
    metaserverS3ClientAdaptor_ = new S3ClientAdaptorImpl();

    mockMetaserverS3Client_ = new MockS3Client();
    metaserverS3ClientAdaptor_->Init(option, mockMetaserverS3Client_);
  }

  void TearDown() override {
    server_.Stop(0);
    server_.Join();
    if (metaserverS3ClientAdaptor_ != nullptr) {
      delete metaserverS3ClientAdaptor_;
    }
  }

 protected:
  S3ClientAdaptor* metaserverS3ClientAdaptor_;
  MockS3Client* mockMetaserverS3Client_;

  client::MockMetaServerService mockMetaServerService_;
  std::string addr_ = "127.0.0.1:5629";
  brpc::Server server_;
};

void InitInode(Inode* inode) {
  inode->set_inodeid(1);
  inode->set_fsid(2);
  inode->set_length(0);
  inode->set_ctime(1623835517);
  inode->set_mtime(1623835517);
  inode->set_atime(1623835517);
  inode->set_uid(1);
  inode->set_gid(1);
  inode->set_mode(1);
  inode->set_nlink(1);
  inode->set_type(dingofs::pb::metaserver::FsFileType::TYPE_S3);

  /*
  1. write 3MB+1 from 0; (write)
  2. write 2MB+1 from 2MB+2; (overwrite）
  3. write 1MB+1 from 4MB+3; (append)
  */
  const uint64_t FileLen = 5 * 1024 * 1024 + 4;
  inode->set_length(FileLen);
  S3ChunkInfoList* s3ChunkInfoList = new S3ChunkInfoList();
  // 1. 1_0_0 1_1_0 1_2_0 1_3_0
  S3ChunkInfo* s3ChunkInfo1 = s3ChunkInfoList->add_s3chunks();
  s3ChunkInfo1->set_chunkid(1);
  s3ChunkInfo1->set_compaction(0);
  s3ChunkInfo1->set_offset(0);
  const uint64_t first_len = 3 * 1024 * 1024 + 1;
  s3ChunkInfo1->set_len(first_len);
  s3ChunkInfo1->set_size(first_len);
  // 2. 1_2_1 1_3_1  2_0_1
  S3ChunkInfo* s3ChunkInfo2 = s3ChunkInfoList->add_s3chunks();
  s3ChunkInfo2->set_chunkid(1);
  s3ChunkInfo2->set_compaction(1);
  s3ChunkInfo2->set_offset(2 * 1024 * 1024 + 2);
  s3ChunkInfo2->set_len(4 * 1024 * 1024 - 2 * 1024 * 1024 - 2);
  s3ChunkInfo2->set_size(4 * 1024 * 1024 - 2 * 1024 * 1024 - 2);
  S3ChunkInfo* s3ChunkInfo3 = s3ChunkInfoList->add_s3chunks();
  s3ChunkInfo3->set_chunkid(2);
  s3ChunkInfo3->set_compaction(1);
  s3ChunkInfo3->set_offset(4 * 1024 * 1024);
  s3ChunkInfo3->set_len(3);
  s3ChunkInfo3->set_size(3);
  // 3. 2_0_1 2_1_1
  S3ChunkInfo* s3ChunkInfo4 = s3ChunkInfoList->add_s3chunks();
  s3ChunkInfo4->set_chunkid(2);
  s3ChunkInfo4->set_compaction(1);
  s3ChunkInfo4->set_offset(4 * 1024 * 1024 + 3);
  s3ChunkInfo4->set_len(1 * 1024 * 1024 + 1);
  s3ChunkInfo4->set_size(1 * 1024 * 1024 + 1);

  inode->mutable_s3chunkinfomap()->insert({0, *s3ChunkInfoList});
}

// delete chunks
TEST_F(MetaserverS3AdaptorTest, test_delete_chunks) {
  // Init
  Inode inode;
  InitInode(&inode);

  // replace s3 delete
  std::function<int(std::string)> delete_object = [](std::string name) {
    LOG(INFO) << "delete object, name:" << name;
    return 0;
  };
  EXPECT_CALL(*mockMetaserverS3Client_, Delete(_))
      .Times(9)
      .WillRepeatedly(Invoke(delete_object));
  int ret = metaserverS3ClientAdaptor_->Delete(inode);
  ASSERT_EQ(ret, 0);
}

TEST_F(MetaserverS3AdaptorTest, test_delete_idempotence) {
  // Init
  Inode inode;
  InitInode(&inode);

  // replace s3 delete
  // when name == fail_del_name, should be delete or not
  const std::string fail_del_name = "2_1_2_0_1";
  bool deleted = true;
  std::set<std::string> deleteObject;
  std::function<int(std::string)> delete_object = [&deleteObject, fail_del_name,
                                                   &deleted](std::string name) {
    int ret = 0;
    if (deleted && fail_del_name == name) {
      LOG(INFO) << "delete object fail, name: " << name;
      deleted = false;
      ret = -1;
    } else {
      LOG(INFO) << "delete object sucess, name: " << name;
      deleteObject.insert(name);
    }
    return ret;
  };
  EXPECT_CALL(*mockMetaserverS3Client_, Delete(_))
      .WillRepeatedly(Invoke(delete_object));

  int ret = 0;
  do {
    ret = metaserverS3ClientAdaptor_->Delete(inode);
  } while (ret < 0);

  ASSERT_EQ(ret, 0);
}

TEST_F(MetaserverS3AdaptorTest, test_delete_deleted) {
  // Init
  Inode inode;
  InitInode(&inode);

  // replace s3 delete
  // when name == fail_del_name, should be deleted or not
  const std::string fail_del_name = "2_1_2_0_1";
  bool deleted = true;
  std::set<std::string> deleteObject;
  std::function<int(std::string)> delete_object = [&deleteObject, fail_del_name,
                                                   &deleted](std::string name) {
    int ret = 0;
    if (deleted && fail_del_name == name) {
      LOG(INFO) << "delete object fail, name: " << name;
      deleted = false;
      ret = 1;
    } else {
      LOG(INFO) << "delete object sucess, name: " << name;
      deleteObject.insert(name);
    }
    return ret;
  };
  EXPECT_CALL(*mockMetaserverS3Client_, Delete(_))
      .WillRepeatedly(Invoke(delete_object));

  int ret = 0;
  do {
    ret = metaserverS3ClientAdaptor_->Delete(inode);
  } while (ret < 0);

  ASSERT_EQ(ret, 0);
}

// delete chunks
TEST_F(MetaserverS3AdaptorTest, test_delete_batch_chunks) {
  S3ClientAdaptorOption option;
  option.blockSize = 1 * 1024 * 1024;
  option.chunkSize = 4 * 1024 * 1024;
  option.batchSize = 5;
  option.objectPrefix = 0;
  option.enableDeleteObjects = true;
  metaserverS3ClientAdaptor_->Init(option, mockMetaserverS3Client_);

  // Init
  Inode inode;
  InitInode(&inode);

  // replace s3 delete
  std::function<int(const std::list<std::string>&)> delete_object =
      [](const std::list<std::string>& nameList) {
        LOG(INFO) << "delete count = " << nameList.size();
        for (const std::string& name : nameList) {
          LOG(INFO) << "delete object, name:" << name;
        }
        return 0;
      };
  EXPECT_CALL(*mockMetaserverS3Client_, DeleteBatch(_))
      .Times(2)
      .WillRepeatedly(Invoke(delete_object));
  int ret = metaserverS3ClientAdaptor_->Delete(inode);
  ASSERT_EQ(ret, 0);
}

TEST_F(MetaserverS3AdaptorTest, test_delete_batch_idempotence) {
  S3ClientAdaptorOption option;
  option.blockSize = 1 * 1024 * 1024;
  option.chunkSize = 4 * 1024 * 1024;
  option.batchSize = 5;
  option.objectPrefix = 0;
  option.enableDeleteObjects = true;
  metaserverS3ClientAdaptor_->Init(option, mockMetaserverS3Client_);

  // Init
  Inode inode;
  InitInode(&inode);

  // replace s3 delete
  // when name == fail_del_name, should be delete or not
  const std::string fail_del_name = "2_1_2_0_1";
  bool deleted = true;
  std::set<std::string> deleteObject;
  std::function<int(const std::list<std::string>&)> delete_object =
      [&deleteObject, fail_del_name,
       &deleted](const std::list<std::string>& nameList) {
        LOG(INFO) << "delete count = " << nameList.size();
        int ret = 0;
        for (const std::string& name : nameList) {
          if (deleted && fail_del_name == name) {
            LOG(INFO) << "delete object fail, name: " << name;
            deleted = false;
            ret = -1;
          } else {
            LOG(INFO) << "delete object sucess, name: " << name;
            deleteObject.insert(name);
          }
        }
        return ret;
      };
  EXPECT_CALL(*mockMetaserverS3Client_, DeleteBatch(_))
      .WillRepeatedly(Invoke(delete_object));

  int ret = 0;
  do {
    ret = metaserverS3ClientAdaptor_->Delete(inode);
  } while (ret < 0);

  ASSERT_EQ(ret, 0);
}

TEST_F(MetaserverS3AdaptorTest, test_delete_batch_deleted) {
  S3ClientAdaptorOption option;
  option.blockSize = 1 * 1024 * 1024;
  option.chunkSize = 4 * 1024 * 1024;
  option.batchSize = 5;
  option.objectPrefix = 0;
  option.enableDeleteObjects = true;
  metaserverS3ClientAdaptor_->Init(option, mockMetaserverS3Client_);

  // Init
  Inode inode;
  InitInode(&inode);

  // replace s3 delete
  // when name == fail_del_name, should be deleted or not
  const std::string fail_del_name = "2_1_2_0_1";
  bool deleted = true;
  std::set<std::string> deleteObject;
  std::function<int(const std::list<std::string>&)> delete_object =
      [&deleteObject, fail_del_name,
       &deleted](const std::list<std::string>& nameList) {
        LOG(INFO) << "delete count = " << nameList.size();
        int ret = 0;
        for (const std::string& name : nameList) {
          if (deleted && fail_del_name == name) {
            LOG(INFO) << "delete object fail, name: " << name;
            deleted = false;
            ret = 1;
          } else {
            LOG(INFO) << "delete object sucess, name: " << name;
            deleteObject.insert(name);
          }
        }
        return ret;
      };
  EXPECT_CALL(*mockMetaserverS3Client_, DeleteBatch(_))
      .WillRepeatedly(Invoke(delete_object));

  int ret = 0;
  do {
    ret = metaserverS3ClientAdaptor_->Delete(inode);
  } while (ret < 0);

  ASSERT_EQ(ret, 0);
}

}  // namespace metaserver
}  // namespace dingofs

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, false);

  return RUN_ALL_TESTS();
}
