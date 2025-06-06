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

#include "metaserver/inode_manager.h"

#include <glog/logging.h>
#include <google/protobuf/util/message_differencer.h>

#include <ctime>

#include "dingofs/metaserver.pb.h"
#include "common/define.h"
#include "metaserver/storage/iterator.h"
#include "stub/filesystem/xattr.h"
#include "utils/concurrent/name_lock.h"
#include "utils/timeutility.h"

namespace dingofs {
namespace metaserver {

using stub::filesystem::XATTR_DIR_ENTRIES;
using stub::filesystem::XATTR_DIR_FBYTES;
using stub::filesystem::XATTR_DIR_FILES;
using stub::filesystem::XATTR_DIR_SUBDIRS;

using storage::Iterator;
using storage::Key4Inode;
using utils::NameLockGuard;
using utils::TimeUtility;

using pb::metaserver::FsFileType;
using pb::metaserver::Inode;
using pb::metaserver::InodeAttr;
using pb::metaserver::ManageInodeType;
using pb::metaserver::MetaStatusCode;
using pb::metaserver::VolumeExtentList;
using pb::metaserver::VolumeExtentSlice;
using pb::metaserver::XAttr;

MetaStatusCode InodeManager::CreateInode(uint64_t inodeId,
                                         const InodeParam& param,
                                         Inode* newInode) {
  VLOG(6) << "CreateInode, fsId = " << param.fsId
          << ", length = " << param.length << ", uid = " << param.uid
          << ", gid = " << param.gid << ", mode = " << param.mode
          << ", type =" << FsFileType_Name(param.type)
          << ", symlink = " << param.symlink << ", rdev = " << param.rdev
          << ", parent = " << param.parent;
  if (param.type == FsFileType::TYPE_SYM_LINK && param.symlink.empty()) {
    return MetaStatusCode::SYM_LINK_EMPTY;
  }

  // 1. generate inode
  Inode inode;
  GenerateInodeInternal(inodeId, param, &inode);
  if (param.type == FsFileType::TYPE_SYM_LINK) {
    inode.set_symlink(param.symlink);
  }

  // 2. insert inode
  MetaStatusCode ret = inodeStorage_->Insert(inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "ret = " << MetaStatusCode_Name(ret)
               << "inode = " << inode.DebugString();
    return ret;
  }

  newInode->CopyFrom(inode);
  ++(*type2InodeNum_)[inode.type()];
  VLOG(9) << "CreateInode success, inode = " << inode.ShortDebugString();
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::CreateRootInode(const InodeParam& param) {
  LOG(INFO) << "CreateRootInode, fsId = " << param.fsId
            << ", uid = " << param.uid << ", gid = " << param.gid
            << ", mode = " << param.mode;

  // 1. generate root inode
  Inode inode;
  GenerateInodeInternal(ROOTINODEID, param, &inode);

  // 2. insert root inode
  MetaStatusCode ret = inodeStorage_->Insert(inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "CreateRootInode fail, fsId = " << param.fsId
               << ", uid = " << param.uid << ", gid = " << param.gid
               << ", mode = " << param.mode
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }
  ++(*type2InodeNum_)[inode.type()];
  LOG(INFO) << "CreateRootInode success, inode: " << inode.ShortDebugString();
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::CreateManageInode(const InodeParam& param,
                                               ManageInodeType manageType,
                                               Inode* newInode) {
  LOG(INFO) << "CreateManageInode, fsId = " << param.fsId
            << ", uid = " << param.uid << ", gid = " << param.gid
            << ", mode = " << param.mode;

  // 1. get inode id
  uint64_t inodeId = 0;
  if (ManageInodeType::TYPE_RECYCLE == manageType) {
    inodeId = RECYCLEINODEID;
  } else {
    LOG(ERROR) << "Create manage inode fail, manageType not support.";
    return MetaStatusCode::PARAM_ERROR;
  }

  // 2. generate manage inode
  Inode inode;
  GenerateInodeInternal(inodeId, param, &inode);

  // 3. insert manage inode
  MetaStatusCode ret = inodeStorage_->Insert(inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "CreateManageInode fail, fsId = " << param.fsId
               << ", uid = " << param.uid << ", gid = " << param.gid
               << ", mode = " << param.mode
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  ++(*type2InodeNum_)[inode.type()];
  newInode->CopyFrom(inode);

  LOG(INFO) << "CreateManageInode success, inode: " << inode.ShortDebugString();
  return MetaStatusCode::OK;
}

void InodeManager::GenerateInodeInternal(uint64_t inodeId,
                                         const InodeParam& param,
                                         Inode* inode) {
  inode->set_inodeid(inodeId);
  inode->set_fsid(param.fsId);
  inode->set_length(param.length);
  inode->set_uid(param.uid);
  inode->set_gid(param.gid);
  inode->set_mode(param.mode);
  inode->set_type(param.type);
  inode->set_rdev(param.rdev);
  inode->add_parent(param.parent);

  if (param.timestamp.has_value()) {
    inode->set_mtime(param.timestamp->tv_sec);
    inode->set_mtime_ns(param.timestamp->tv_nsec);
    inode->set_atime(param.timestamp->tv_sec);
    inode->set_atime_ns(param.timestamp->tv_nsec);
    inode->set_ctime(param.timestamp->tv_sec);
    inode->set_ctime_ns(param.timestamp->tv_nsec);
  } else {
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    inode->set_mtime(now.tv_sec);
    inode->set_mtime_ns(now.tv_nsec);
    inode->set_atime(now.tv_sec);
    inode->set_atime_ns(now.tv_nsec);
    inode->set_ctime(now.tv_sec);
    inode->set_ctime_ns(now.tv_nsec);
  }

  if (FsFileType::TYPE_DIRECTORY == param.type) {
    inode->set_nlink(2);
    // set summary xattr
    inode->mutable_xattr()->insert({XATTR_DIR_FILES, "0"});
    inode->mutable_xattr()->insert({XATTR_DIR_SUBDIRS, "0"});
    inode->mutable_xattr()->insert({XATTR_DIR_ENTRIES, "0"});
    inode->mutable_xattr()->insert({XATTR_DIR_FBYTES, "0"});
  } else {
    inode->set_nlink(1);
  }
}

MetaStatusCode InodeManager::GetInode(uint32_t fs_id, uint64_t inode_id,
                                      Inode* inode,
                                      bool padding_s3_chunk_info) {
  VLOG(6) << "GetInode, fsId = " << fs_id << ", inodeId = " << inode_id;
  NameLockGuard lg(inodeLock_, GetInodeLockName(fs_id, inode_id));
  MetaStatusCode rc = inodeStorage_->Get(Key4Inode(fs_id, inode_id), inode);
  if (rc == MetaStatusCode::OK && padding_s3_chunk_info) {
    rc = PaddingInodeS3ChunkInfo(fs_id, inode_id,
                                 inode->mutable_s3chunkinfomap());
  }

  if (rc != MetaStatusCode::OK) {
    std::ostringstream oss;
    oss << "GetInode fail, fsId = " << fs_id << ", inodeId = " << inode_id
        << ", retCode = " << MetaStatusCode_Name(rc);

    if (rc == MetaStatusCode::STORAGE_CLOSED) {
      LOG(WARNING) << oss.str();
    } else {
      LOG(ERROR) << oss.str();
    }
    return rc;
  }

  VLOG(9) << "GetInode success, fsId = " << fs_id << ", inodeId = " << inode_id
          << ", " << inode->ShortDebugString();
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::GetInodeAttr(uint32_t fsId, uint64_t inodeId,
                                          InodeAttr* attr) {
  VLOG(6) << "GetInodeAttr, fsId = " << fsId << ", inodeId = " << inodeId;
  NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
  MetaStatusCode ret = inodeStorage_->GetAttr(Key4Inode(fsId, inodeId), attr);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "GetInodeAttr fail, fsId = " << fsId
               << ", inodeId = " << inodeId
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  VLOG(9) << "GetInodeAttr success, fsId = " << fsId
          << ", inodeId = " << inodeId << ", " << attr->ShortDebugString();

  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::GetXAttr(uint32_t fsId, uint64_t inodeId,
                                      XAttr* xattr) {
  VLOG(6) << "GetXAttr, fsId = " << fsId << ", inodeId = " << inodeId;
  NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
  xattr->set_inodeid(inodeId);
  xattr->set_fsid(fsId);
  MetaStatusCode ret = inodeStorage_->GetXAttr(Key4Inode(fsId, inodeId), xattr);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "GetXAttr fail, fsId = " << fsId << ", inodeId = " << inodeId
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  VLOG(9) << "GetXAttr success, fsId = " << fsId << ", inodeId = " << inodeId
          << ", " << xattr->ShortDebugString();

  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::DeleteInode(uint32_t fsId, uint64_t inodeId) {
  VLOG(6) << "DeleteInode, fsId = " << fsId << ", inodeId = " << inodeId;
  NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));
  InodeAttr attr;
  MetaStatusCode retGetAttr =
      inodeStorage_->GetAttr(Key4Inode(fsId, inodeId), &attr);
  if (retGetAttr != MetaStatusCode::OK) {
    VLOG(9) << "GetInodeAttr fail, fsId = " << fsId << ", inodeId = " << inodeId
            << ", ret = " << MetaStatusCode_Name(retGetAttr);
  }

  MetaStatusCode ret = inodeStorage_->Delete(Key4Inode(fsId, inodeId));
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "DeleteInode fail, fsId = " << fsId
               << ", inodeId = " << inodeId
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  if (retGetAttr == MetaStatusCode::OK) {
    // get attr success
    --(*type2InodeNum_)[attr.type()];
  }
  VLOG(6) << "DeleteInode success, fsId = " << fsId
          << ", inodeId = " << inodeId;
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::UpdateInode(
    const pb::metaserver::UpdateInodeRequest& request) {
  VLOG(9) << "update inode, fsid: " << request.fsid()
          << ", inodeid: " << request.inodeid();
  NameLockGuard lg(inodeLock_,
                   GetInodeLockName(request.fsid(), request.inodeid()));

  Inode old;
  MetaStatusCode ret =
      inodeStorage_->Get(Key4Inode(request.fsid(), request.inodeid()), &old);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "GetInode fail, " << request.ShortDebugString()
               << ", ret: " << MetaStatusCode_Name(ret);
    return ret;
  }

  bool needUpdate = false;
  bool needAddTrash = false;

#define UPDATE_INODE(param)           \
  if (request.has_##param()) {        \
    old.set_##param(request.param()); \
    needUpdate = true;                \
  }

  UPDATE_INODE(length)
  UPDATE_INODE(ctime)
  UPDATE_INODE(ctime_ns)
  UPDATE_INODE(mtime)
  UPDATE_INODE(mtime_ns)
  UPDATE_INODE(atime)
  UPDATE_INODE(atime_ns)
  UPDATE_INODE(uid)
  UPDATE_INODE(gid)
  UPDATE_INODE(mode)

  if (request.parent_size() > 0) {
    *(old.mutable_parent()) = request.parent();
    needUpdate = true;
  }

  if (request.has_nlink()) {
    if (old.nlink() != 0 && request.nlink() == 0) {
      uint32_t now = TimeUtility::GetTimeofDaySec();
      old.set_dtime(now);
      needAddTrash = true;
    }
    VLOG(9) << "update inode nlink, from " << old.nlink() << " to "
            << request.nlink() << ", fsid: " << request.fsid()
            << ", inodeid: " << request.inodeid();
    old.set_nlink(request.nlink());
    needUpdate = true;
  }

  if (!request.xattr().empty()) {
    VLOG(6) << "update inode has xattr, fsid: " << request.fsid()
            << ", inodeid: " << request.inodeid();
    *(old.mutable_xattr()) = request.xattr();
    needUpdate = true;
  }

  if (needUpdate) {
    ret = inodeStorage_->Update(old);
    if (ret != MetaStatusCode::OK) {
      LOG(ERROR) << "UpdateInode fail, " << request.ShortDebugString()
                 << ", ret: " << MetaStatusCode_Name(ret);
      return ret;
    }
  }

  if (needAddTrash) {
    trash_->Add(old.fsid(), old.inodeid(), old.dtime());
    --(*type2InodeNum_)[old.type()];
  }

  const S3ChunkInfoMap& map2add = request.s3chunkinfoadd();
  const S3ChunkInfoList* list2add;
  VLOG(9) << "UpdateInode inode " << old.inodeid() << " map2add size "
          << map2add.size();
  for (const auto& item : map2add) {
    uint64_t chunkIndex = item.first;
    list2add = &item.second;
    MetaStatusCode rc = inodeStorage_->ModifyInodeS3ChunkInfoList(
        old.fsid(), old.inodeid(), chunkIndex, list2add, nullptr);
    if (rc != MetaStatusCode::OK) {
      LOG(ERROR) << "Modify inode s3chunkinfo list failed, fsId=" << old.fsid()
                 << ", inodeId=" << old.inodeid() << ", retCode=" << rc;
      return rc;
    }
  }

  // update extent in request
  if (request.has_volumeextents()) {
    const auto fsId = old.fsid();
    const auto inodeId = old.inodeid();
    for (const auto& slice : request.volumeextents().slices()) {
      auto rc = UpdateVolumeExtentSliceLocked(fsId, inodeId, slice);
      if (rc != MetaStatusCode::OK) {
        LOG(ERROR) << "UpdateVolumeExtent failed, err: "
                   << MetaStatusCode_Name(rc) << ", fsId: " << fsId
                   << ", inodeId: " << inodeId;
        return rc;
      }
    }
  }

  VLOG(9) << "UpdateInode success, " << request.ShortDebugString();
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::GetOrModifyS3ChunkInfo(
    uint32_t fsId, uint64_t inodeId, const S3ChunkInfoMap& map2add,
    const S3ChunkInfoMap& map2del, bool returnS3ChunkInfoMap,
    std::shared_ptr<Iterator>* iterator4InodeS3Meta) {
  VLOG(6) << "GetOrModifyS3ChunkInfo, fsId: " << fsId
          << ", inodeId: " << inodeId;

  NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));

  const S3ChunkInfoList* list2add;
  const S3ChunkInfoList* list2del;
  std::unordered_set<uint64_t> deleted;
  for (const auto& item : map2add) {
    uint64_t chunkIndex = item.first;
    list2add = &item.second;
    auto iter = map2del.find(chunkIndex);
    if (iter != map2del.end()) {
      list2del = &iter->second;
    } else {
      list2del = nullptr;
    }

    MetaStatusCode rc = inodeStorage_->ModifyInodeS3ChunkInfoList(
        fsId, inodeId, chunkIndex, list2add, list2del);
    if (rc != MetaStatusCode::OK) {
      LOG(ERROR) << "Modify inode s3chunkinfo list failed, fsId=" << fsId
                 << ", inodeId=" << inodeId << ", retCode=" << rc;
      return rc;
    }
    deleted.insert(chunkIndex);
  }

  for (const auto& item : map2del) {
    uint64_t chunkIndex = item.first;
    if (deleted.find(chunkIndex) != deleted.end()) {  // already deleted
      continue;
    }

    list2add = nullptr;
    list2del = &item.second;
    MetaStatusCode rc = inodeStorage_->ModifyInodeS3ChunkInfoList(
        fsId, inodeId, chunkIndex, list2add, list2del);
    if (rc != MetaStatusCode::OK) {
      LOG(ERROR) << "Modify inode s3chunkinfo list failed, fsId=" << fsId
                 << ", inodeId=" << inodeId << ", retCode=" << rc;
      return rc;
    }
  }

  // return if needed
  if (returnS3ChunkInfoMap) {
    *iterator4InodeS3Meta =
        inodeStorage_->GetInodeS3ChunkInfoList(fsId, inodeId);
    if ((*iterator4InodeS3Meta)->Status() != 0) {
      return MetaStatusCode::STORAGE_INTERNAL_ERROR;
    }
  }

  VLOG(6) << "GetOrModifyS3ChunkInfo success, fsId: " << fsId
          << ", inodeId: " << inodeId;
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::PaddingInodeS3ChunkInfo(int32_t fsId,
                                                     uint64_t inodeId,
                                                     S3ChunkInfoMap* m,
                                                     uint64_t limit) {
  VLOG(6) << "PaddingInodeS3ChunkInfo, fsId: " << fsId
          << ", inodeId: " << inodeId;
  return inodeStorage_->PaddingInodeS3ChunkInfo(fsId, inodeId, m, limit);
}

MetaStatusCode InodeManager::UpdateInodeWhenCreateOrRemoveSubNode(
    uint32_t fsId, uint64_t inodeId, FsFileType type, bool isCreate) {
  VLOG(6) << "UpdateInodeWhenCreateOrRemoveSubNode, fsId = " << fsId
          << ", inodeId = " << inodeId << ", isCreate = " << isCreate;
  NameLockGuard lg(inodeLock_, GetInodeLockName(fsId, inodeId));

  Inode inode;
  MetaStatusCode ret = inodeStorage_->Get(Key4Inode(fsId, inodeId), &inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "GetInode fail, " << inode.ShortDebugString()
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  uint32_t oldNlink = inode.nlink();
  if (oldNlink == 0) {
    LOG(ERROR) << "UpdateInodeWhenCreateOrRemoveSubNode already be deleted!"
               << " fsId: " << fsId << ", inodeId: " << inodeId
               << ", type: " << type << ", isCreate: " << isCreate;
    // already be deleted
    return MetaStatusCode::OK;
  }

  if (FsFileType::TYPE_DIRECTORY == type) {
    if (isCreate) {
      inode.set_nlink(++oldNlink);
    } else {
      inode.set_nlink(--oldNlink);
    }
  }

  ret = inodeStorage_->Update(inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "UpdateInode fail, " << inode.ShortDebugString()
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  VLOG(9) << "UpdateInodeWhenCreateOrRemoveSubNode success, "
          << inode.ShortDebugString();
  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::InsertInode(const Inode& inode) {
  VLOG(6) << "InsertInode, " << inode.ShortDebugString();

  // 2. insert inode
  MetaStatusCode ret = inodeStorage_->Insert(inode);
  if (ret != MetaStatusCode::OK) {
    LOG(ERROR) << "InsertInode fail, " << inode.ShortDebugString()
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }

  if (inode.nlink() == 0) {
    trash_->Add(inode.fsid(), inode.inodeid(), inode.dtime());
  }

  return MetaStatusCode::OK;
}

bool InodeManager::GetInodeIdList(std::list<uint64_t>* inodeIdList) {
  return inodeStorage_->GetAllInodeId(inodeIdList);
}

MetaStatusCode InodeManager::UpdateVolumeExtentSliceLocked(
    uint32_t fsId, uint64_t inodeId, const VolumeExtentSlice& slice) {
  return inodeStorage_->UpdateVolumeExtentSlice(fsId, inodeId, slice);
}

MetaStatusCode InodeManager::UpdateVolumeExtentSlice(
    uint32_t fsId, uint64_t inodeId, const VolumeExtentSlice& slice) {
  VLOG(6) << "UpdateInodeExtent, fsId: " << fsId << ", inodeId: " << inodeId
          << ", slice offset: " << slice.offset();
  NameLockGuard guard(inodeLock_, GetInodeLockName(fsId, inodeId));
  return UpdateVolumeExtentSliceLocked(fsId, inodeId, slice);
}

MetaStatusCode InodeManager::UpdateVolumeExtent(
    uint32_t fsId, uint64_t inodeId, const VolumeExtentList& extents) {
  VLOG(6) << "UpdateInodeExtent, fsId: " << fsId << ", inodeId: " << inodeId;
  NameLockGuard guard(inodeLock_, GetInodeLockName(fsId, inodeId));

  MetaStatusCode st = MetaStatusCode::UNKNOWN_ERROR;
  for (const auto& slice : extents.slices()) {
    st = UpdateVolumeExtentSliceLocked(fsId, inodeId, slice);
    if (st != MetaStatusCode::OK) {
      LOG(ERROR) << "UpdateVolumeExtent failed, err: "
                 << MetaStatusCode_Name(st) << ", fsId: " << fsId
                 << ", inodeId: " << inodeId;
      return st;
    }
  }

  return MetaStatusCode::OK;
}

MetaStatusCode InodeManager::GetVolumeExtent(
    uint32_t fsId, uint64_t inodeId, const std::vector<uint64_t>& slices,
    VolumeExtentList* extents) {
  VLOG(6) << "GetInodeExtent, fsId: " << fsId << ", inodeId: " << inodeId;

  if (slices.empty()) {
    return inodeStorage_->GetAllVolumeExtent(fsId, inodeId, extents);
  }

  for (const auto& slice : slices) {
    auto st = inodeStorage_->GetVolumeExtentByOffset(fsId, inodeId, slice,
                                                     extents->add_slices());
    if (st != MetaStatusCode::OK && st != MetaStatusCode::NOT_FOUND) {
      LOG(ERROR) << "GetVolumeExtent failed, fsId: " << fsId
                 << ", inodeId: " << inodeId << ", slice offset: " << slice
                 << ", error: " << MetaStatusCode_Name(st);
      return st;
    }
  }

  return MetaStatusCode::OK;
}

}  // namespace metaserver
}  // namespace dingofs
