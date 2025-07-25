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
 * Created Date: Thur May 27 2021
 * Author: xuchaojie
 */

#ifndef DINGOFS_SRC_CLIENT_INODE_WRAPPER_H_
#define DINGOFS_SRC_CLIENT_INODE_WRAPPER_H_

#include <gtest/gtest_prod.h>
#include <sys/stat.h>

#include <climits>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "client/vfs_legacy/common/common.h"
#include "client/vfs_legacy/filesystem/error.h"
#include "dingofs/metaserver.pb.h"
#include "metrics/client/vfs_legacy/s3_chunk_info.h"
#include "stub/rpcclient/metaserver_client.h"
#include "utils/concurrent/concurrent.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace client {

constexpr int kAccessTime = 1 << 0;
constexpr int kChangeTime = 1 << 1;
constexpr int kModifyTime = 1 << 2;

using filesystem::DINGOFS_ERROR;

enum class InodeStatus {
  kNormal = 0,
  kError = -1,
};

std::ostream& operator<<(std::ostream& os, const struct stat& attr);
void AppendS3ChunkInfoToMap(
    uint64_t chunkIndex, const pb::metaserver::S3ChunkInfo& info,
    google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>*
        s3ChunkInfoMap);

extern bvar::Adder<int64_t> g_alive_inode_count;

class InodeWrapper : public std::enable_shared_from_this<InodeWrapper> {
 public:
  InodeWrapper(pb::metaserver::Inode inode,
               std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient,
               std::shared_ptr<metrics::client::vfs_legacy::S3ChunkInfoMetric>
                   s3ChunkInfoMetric = nullptr,
               int64_t maxDataSize = LONG_MAX,
               uint32_t refreshDataInterval = UINT_MAX)
      : inode_(std::move(inode)),
        status_(InodeStatus::kNormal),
        baseMaxDataSize_(maxDataSize),
        maxDataSize_(maxDataSize),
        refreshDataInterval_(refreshDataInterval),
        lastRefreshTime_(utils::TimeUtility::GetTimeofDaySec()),
        s3ChunkInfoAddSize_(0),
        metaClient_(std::move(metaClient)),
        s3ChunkInfoMetric_(std::move(s3ChunkInfoMetric)),
        dirty_(false),
        time_(utils::TimeUtility::GetTimeofDaySec()) {
    UpdateS3ChunkInfoMetric(CalS3ChunkInfoSize());
    g_alive_inode_count << 1;
  }

  ~InodeWrapper() {
    UpdateS3ChunkInfoMetric(-s3ChunkInfoSize_ - s3ChunkInfoAddSize_);
    g_alive_inode_count << -1;
  }

  uint64_t GetInodeId() const { return inode_.inodeid(); }

  uint32_t GetFsId() const { return inode_.fsid(); }

  pb::metaserver::FsFileType GetType() const { return inode_.type(); }

  std::string GetSymlinkStr() const {
    dingofs::utils::UniqueLock lg(mtx_);
    return inode_.symlink();
  }

  void SetLength(uint64_t len) {
    dingofs::utils::UniqueLock lg(mtx_);
    SetLengthLocked(len);
  }

  void SetLengthLocked(uint64_t len) {
    inode_.set_length(len);
    dirtyAttr_.set_length(len);
    dirty_ = true;
  }

  void SetType(pb::metaserver::FsFileType type) {
    inode_.set_type(type);
    dirtyAttr_.set_type(type);
    dirty_ = true;
  }

  uint64_t GetLengthLocked() const { return inode_.length(); }

  uint64_t GetLength() const {
    dingofs::utils::UniqueLock lg(mtx_);
    return inode_.length();
  }

  void SetUid(uint32_t uid) {
    inode_.set_uid(uid);
    dirtyAttr_.set_uid(uid);
    dirty_ = true;
  }

  void SetGid(uint32_t gid) {
    inode_.set_gid(gid);
    dirtyAttr_.set_gid(gid);
    dirty_ = true;
  }

  void SetMode(uint32_t mode) {
    inode_.set_mode(mode);
    dirtyAttr_.set_mode(mode);
    dirty_ = true;
  }

  pb::metaserver::Inode GetInode() const {
    dingofs::utils::UniqueLock lg(mtx_);
    return inode_;
  }

  timespec GetMtime() const {
    timespec mtime;
    dingofs::utils::UniqueLock lg(mtx_);
    mtime.tv_sec = inode_.mtime();
    mtime.tv_nsec = inode_.mtime_ns();
    return mtime;
  }

  // Get an immutable inode.
  //
  // The const return value is used to forbid modify inode through this
  // interface, all modification operations should using `SetXXX()`.
  const pb::metaserver::Inode* GetInodeLocked() const { return &inode_; }

  // Update timestamp of inode.
  //
  // flags can be any combination of kAccessTime/kModifyTime/kChangeTime
  void UpdateTimestampLocked(int flags);
  void UpdateTimestampLocked(const timespec& now, int flags);

  // Merge incoming extended attributes.
  //
  // Existing attributes will be overwritten, new attributes well be inserted.
  void MergeXAttrLocked(
      const google::protobuf::Map<std::string, std::string>& xattrs);

  DINGOFS_ERROR GetInodeAttrUnLocked(pb::metaserver::InodeAttr* attr);

  void GetInodeAttr(pb::metaserver::InodeAttr* attr);

  pb::metaserver::XAttr GetXattr() const {
    pb::metaserver::XAttr ret;
    dingofs::utils::UniqueLock lg(mtx_);
    ret.set_fsid(inode_.fsid());
    ret.set_inodeid(inode_.inodeid());
    *(ret.mutable_xattrinfos()) = inode_.xattr();
    return ret;
  }

  void SetXattrLocked(const std::string& key, const std::string& value) {
    (*inode_.mutable_xattr())[key] = value;
    (*dirtyAttr_.mutable_xattr()) = inode_.xattr();
    dirty_ = true;
  }

  dingofs::utils::UniqueLock GetUniqueLock() {
    return dingofs::utils::UniqueLock(mtx_);
  }

  const google::protobuf::RepeatedField<uint64_t>& GetParentLocked() {
    return inode_.parent();
  }

  DINGOFS_ERROR UpdateParent(uint64_t oldParent, uint64_t newParent);

  // dir will not update parent
  DINGOFS_ERROR Link(uint64_t parent = 0);

  DINGOFS_ERROR UnLinkWithReturn(uint64_t parent, uint32_t& out_nlink);
  DINGOFS_ERROR UnLink(uint64_t parent);

  DINGOFS_ERROR RefreshNlink();

  DINGOFS_ERROR Sync(bool internal = false);

  DINGOFS_ERROR SyncS3(bool internal = false);

  void Async(stub::rpcclient::MetaServerClientDone* done,
             bool internal = false);

  void AsyncS3(stub::rpcclient::MetaServerClientDone* done,
               bool internal = false);

  DINGOFS_ERROR SyncAttr(bool internal = false);

  void AsyncFlushAttr(stub::rpcclient::MetaServerClientDone* done,
                      bool internal);

  void FlushS3ChunkInfoAsync();

  DINGOFS_ERROR RefreshS3ChunkInfo();

  DINGOFS_ERROR Open();

  bool IsOpen();

  DINGOFS_ERROR Release();

  void MarkDirty() { dirty_ = true; }

  bool IsDirty() const { return dirty_; }

  void ClearDirty() { dirty_ = false; }

  bool S3ChunkInfoEmpty() {
    dingofs::utils::UniqueLock lg(mtx_);
    return s3ChunkInfoAdd_.empty();
  }

  bool S3ChunkInfoEmptyNolock() { return s3ChunkInfoAdd_.empty(); }

  uint32_t GetNlinkLocked() { return inode_.nlink(); }

  void UpdateNlinkLocked(common::NlinkChange nlink) {
    inode_.set_nlink(inode_.nlink() + static_cast<int32_t>(nlink));
  }

  void AppendS3ChunkInfo(uint64_t chunkIndex,
                         const pb::metaserver::S3ChunkInfo& info) {
    dingofs::utils::UniqueLock lg(mtx_);
    AppendS3ChunkInfoToMap(chunkIndex, info, &s3ChunkInfoAdd_);
    AppendS3ChunkInfoToMap(chunkIndex, info, inode_.mutable_s3chunkinfomap());
    s3ChunkInfoAddSize_++;
    s3ChunkInfoSize_++;
    UpdateS3ChunkInfoMetric(2);
  }

  google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>*
  GetChunkInfoMap() {
    return inode_.mutable_s3chunkinfomap();
  }

  void MarkInodeError() {
    // TODO(xuchaojie) : when inode is marked error, prevent futher write.
    status_ = InodeStatus::kError;
  }

  void LockSyncingInode() const { syncingInodeMtx_.lock(); }

  void ReleaseSyncingInode() const { syncingInodeMtx_.unlock(); }

  dingofs::utils::UniqueLock GetSyncingInodeUniqueLock() {
    return dingofs::utils::UniqueLock(syncingInodeMtx_);
  }

  void LockSyncingS3ChunkInfo() const { syncingS3ChunkInfoMtx_.lock(); }

  void ReleaseSyncingS3ChunkInfo() const { syncingS3ChunkInfoMtx_.unlock(); }

  dingofs::utils::UniqueLock GetSyncingS3ChunkInfoUniqueLock() {
    return dingofs::utils::UniqueLock(syncingS3ChunkInfoMtx_);
  }

  bool NeedRefreshData() {
    if (s3ChunkInfoSize_ >= maxDataSize_ &&
        ::dingofs::utils::TimeUtility::GetTimeofDaySec() - lastRefreshTime_ >=
            refreshDataInterval_) {
      VLOG(6) << "EliminateLargeS3ChunkInfo size = " << s3ChunkInfoSize_
              << ", max = " << maxDataSize_
              << ", inodeId = " << inode_.inodeid();
      return true;
    }
    return false;
  }

  uint32_t GetCachedTime() const { return time_; }

 private:
  friend class UpdateVolumeExtentClosure;
  friend class UpdateInodeAttrAndExtentClosure;

  FRIEND_TEST(TestInodeWrapper, TestUpdateInodeAttrIncrementally);

  DINGOFS_ERROR SyncS3ChunkInfo(bool internal = false);

  int64_t CalS3ChunkInfoSize() {
    int64_t size = 0;
    for (const auto& it : inode_.s3chunkinfomap()) {
      size += it.second.s3chunks_size();
    }
    s3ChunkInfoSize_ = size;
    return s3ChunkInfoSize_;
  }

  void UpdateS3ChunkInfoMetric(int64_t count) {
    if (nullptr != s3ChunkInfoMetric_) {
      s3ChunkInfoMetric_->s3ChunkInfoSize << count;
    }
  }

  void ClearS3ChunkInfoAdd() {
    UpdateS3ChunkInfoMetric(-s3ChunkInfoAddSize_);
    s3ChunkInfoAdd_.clear();
    s3ChunkInfoAddSize_ = 0;
  }

  void UpdateMaxS3ChunkInfoSize() {
    VLOG(6) << "UpdateMaxS3ChunkInfoSize size = " << s3ChunkInfoSize_
            << ", max = " << maxDataSize_ << ", inodeId = " << inode_.inodeid();
    if (s3ChunkInfoSize_ < baseMaxDataSize_) {
      maxDataSize_ = baseMaxDataSize_;
    } else {
      maxDataSize_ = s3ChunkInfoSize_;
    }
  }

  // Flush inode attributes and extents asynchronously.
  // REQUIRES: |mtx_| is held
  void AsyncFlushAttrAndExtents(stub::rpcclient::MetaServerClientDone* done,
                                bool internal);

  pb::metaserver::Inode inode_;

  // dirty attributes, and needs update to metaserver
  pb::metaserver::InodeAttr dirtyAttr_;

  InodeStatus status_;
  int64_t baseMaxDataSize_;
  int64_t maxDataSize_;
  uint32_t refreshDataInterval_;
  uint64_t lastRefreshTime_;

  google::protobuf::Map<uint64_t, pb::metaserver::S3ChunkInfoList>
      s3ChunkInfoAdd_;
  int64_t s3ChunkInfoAddSize_;
  int64_t s3ChunkInfoSize_;

  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;
  std::shared_ptr<metrics::client::vfs_legacy::S3ChunkInfoMetric>
      s3ChunkInfoMetric_;
  bool dirty_;
  mutable utils::Mutex mtx_;

  mutable utils::Mutex syncingInodeMtx_;
  mutable utils::Mutex syncingS3ChunkInfoMtx_;

  mutable utils::Mutex syncingVolumeExtentsMtx_;

  // timestamp when put in cache
  uint64_t time_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_INODE_WRAPPER_H_
