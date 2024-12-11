/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: curve
 * Created Date: Thur May 12 2022
 * Author: wanghai01
 */

#include "curvefs/src/client/xattr_manager.h"

#include "curvefs/src/client/common/common.h"
#include "curvefs/src/stub/filesystem/xattr.h"
#include "glog/logging.h"
#include "curvefs/src/utils/string_util.h"

namespace curvefs {
namespace client {

using ::curvefs::utils::StringToUll;
using ::curvefs::utils::Thread;
using ::curvefs::client::common::AddUllStringToFirst;
using ::curvefs::stub::filesystem::XATTR_DIR_ENTRIES;
using ::curvefs::stub::filesystem::XATTR_DIR_FBYTES;
using ::curvefs::stub::filesystem::XATTR_DIR_FILES;
using ::curvefs::stub::filesystem::XATTR_DIR_PREFIX;
using ::curvefs::stub::filesystem::XATTR_DIR_RENTRIES;
using ::curvefs::stub::filesystem::XATTR_DIR_RFBYTES;
using ::curvefs::stub::filesystem::XATTR_DIR_RFILES;
using ::curvefs::stub::filesystem::XATTR_DIR_RSUBDIRS;
using ::curvefs::stub::filesystem::XATTR_DIR_SUBDIRS;

bool IsSummaryInfo(const char* name) {
  return std::strstr(name, XATTR_DIR_PREFIX);
}

bool IsOneLayer(const char* name) {
  if (std::strcmp(name, XATTR_DIR_FILES) == 0 ||
      std::strcmp(name, XATTR_DIR_SUBDIRS) == 0 ||
      std::strcmp(name, XATTR_DIR_ENTRIES) == 0 ||
      std::strcmp(name, XATTR_DIR_FBYTES) == 0) {
    return true;
  }
  return false;
}

CURVEFS_ERROR XattrManager::CalOneLayerSumInfo(InodeAttr* attr) {
  std::stack<uint64_t> iStack;
  // use set can deal with hard link
  std::set<uint64_t> inodeIds;
  std::list<InodeAttr> attrs;
  auto ino = attr->inodeid();

  std::list<Dentry> dentryList;
  auto ret =
      dentryManager_->ListDentry(ino, &dentryList, listDentryLimit_, false);
  if (CURVEFS_ERROR::OK != ret) {
    LOG(ERROR) << "ListDentry failed, inodeId = " << ino
               << ", limit = " << listDentryLimit_ << ", onlyDir = false";
    return ret;
  }

  for (const auto& it : dentryList) {
    inodeIds.emplace(it.inodeid());
  }

  ret = inodeManager_->BatchGetInodeAttr(&inodeIds, &attrs);
  if (ret == CURVEFS_ERROR::OK) {
    SummaryInfo summaryInfo;
    for (const auto& it : attrs) {
      if (it.type() == FsFileType::TYPE_DIRECTORY) {
        summaryInfo.subdirs++;
      } else {
        summaryInfo.files++;
      }
      summaryInfo.entries++;
      summaryInfo.fbytes += it.length();
    }
    (*attr->mutable_xattr())[XATTR_DIR_FILES] =
        std::to_string(summaryInfo.files);
    (*attr->mutable_xattr())[XATTR_DIR_SUBDIRS] =
        std::to_string(summaryInfo.subdirs);
    (*attr->mutable_xattr())[XATTR_DIR_ENTRIES] =
        std::to_string(summaryInfo.entries);
    (*attr->mutable_xattr())[XATTR_DIR_FBYTES] =
        std::to_string(summaryInfo.fbytes + attr->length());
  }
  return ret;
}

CURVEFS_ERROR XattrManager::FastCalOneLayerSumInfo(InodeAttr* attr) {
  if (!AddUllStringToFirst(
          &(attr->mutable_xattr()->find(XATTR_DIR_FBYTES)->second),
          attr->length(), true)) {
    return CURVEFS_ERROR::INTERNAL;
  }
  return CURVEFS_ERROR::OK;
}

bool XattrManager::ConcurrentListDentry(std::list<Dentry>* dentrys,
                                        std::stack<uint64_t>* iStack,
                                        std::mutex* stackMutex, bool dirOnly,
                                        Atomic<uint32_t>* inflightNum,
                                        Atomic<bool>* ret) {
  InterruptibleSleeper sleeper;
  uint64_t sleepIntervalMs = 5;
  while (1) {
    uint64_t ino = 0;
    stackMutex->lock();
    // 1. fuse client stop
    // 2. if any of request failed, the upper request failed.
    // 3. iStack is empty && inflightNum = 0 means finished.
    if (isStop_.load() || !ret->load() ||
        (iStack->empty() && inflightNum->load() == 0)) {
      stackMutex->unlock();
      return false;
    }

    if (!iStack->empty()) {
      ino = iStack->top();
      iStack->pop();
      inflightNum->fetch_add(1);
      stackMutex->unlock();
    } else {
      stackMutex->unlock();
      sleeper.wait_for(std::chrono::milliseconds(sleepIntervalMs));
      continue;
    }

    // if onlydir, can get parent nlink to know dir number under this dir
    uint32_t nlink = 0;
    if (dirOnly) {
      InodeAttr attr;
      auto retCode = inodeManager_->GetInodeAttr(ino, &attr);
      if (retCode != CURVEFS_ERROR::OK) {
        LOG(ERROR) << "inodeManager get inodeAttr fail, ret = " << retCode
                   << ", inodeid = " << ino;
        ret->store(false);
        inflightNum->fetch_sub(1);
        return false;
      }
      nlink = attr.nlink();
    }

    auto tret = dentryManager_->ListDentry(ino, dentrys, listDentryLimit_,
                                           dirOnly, nlink);
    if (CURVEFS_ERROR::OK != tret) {
      LOG(ERROR) << "ListDentry failed, inodeId = " << ino
                 << ", limit = " << listDentryLimit_
                 << ", onlyDir = " << dirOnly << ", ret = " << tret;
      ret->store(false);
      inflightNum->fetch_sub(1);
      return false;
    }
    return true;
  }
}

void XattrManager::ConcurrentGetInodeAttr(
    std::stack<uint64_t>* iStack, std::mutex* stackMutex,
    std::unordered_map<uint64_t, uint64_t>* hardLinkMap, std::mutex* mapMutex,
    SummaryInfo* summaryInfo, std::mutex* valueMutex,
    Atomic<uint32_t>* inflightNum, Atomic<bool>* ret) {
  while (1) {
    std::list<Dentry> dentryList;
    std::set<uint64_t> inodeIds;
    std::list<InodeAttr> attrs;
    auto tret = ConcurrentListDentry(&dentryList, iStack, stackMutex, false,
                                     inflightNum, ret);
    if (!tret) {
      return;
    }
    {
      std::lock_guard<std::mutex> guard(*stackMutex);
      for (const auto& it : dentryList) {
        if (it.type() == FsFileType::TYPE_DIRECTORY) {
          iStack->emplace(it.inodeid());
        }
        inodeIds.emplace(it.inodeid());
      }
      inflightNum->fetch_sub(1);
    }
    if (!inodeIds.empty()) {
      auto tret = inodeManager_->BatchGetInodeAttr(&inodeIds, &attrs);
      if (tret == CURVEFS_ERROR::OK) {
        std::lock_guard<std::mutex> guard(*valueMutex);
        for (const auto& it : attrs) {
          if (it.type() == FsFileType::TYPE_DIRECTORY) {
            summaryInfo->subdirs++;
          } else {
            summaryInfo->files++;
          }
          summaryInfo->entries++;
          summaryInfo->fbytes += it.length();
          // record hardlink
          if (it.type() != FsFileType::TYPE_DIRECTORY && it.nlink() > 1) {
            std::lock_guard<std::mutex> guard(*mapMutex);
            auto iter = hardLinkMap->find(it.inodeid());
            if (iter != hardLinkMap->end()) {
              iter->second += it.length();
            } else {
              hardLinkMap->emplace(it.inodeid(), 0);
            }
          }
        }
      } else {
        ret->store(false);
        return;
      }
    }
  }
}

CURVEFS_ERROR XattrManager::CalAllLayerSumInfo(InodeAttr* attr) {
  std::stack<uint64_t> iStack;
  std::mutex stackMutex;

  // record hard link, <inodeId, need2minus>
  std::unordered_map<uint64_t, uint64_t> hardLinkMap;
  std::mutex mapMutex;

  SummaryInfo summaryInfo;
  std::mutex valueMutex;

  auto ino = attr->inodeid();
  iStack.emplace(ino);
  std::vector<Thread> threadpool;
  Atomic<uint32_t> inflightNum(0);
  Atomic<bool> ret(true);

  for (auto i = listDentryThreads_; i > 0; i--) {
    try {
      threadpool.emplace_back(Thread(&XattrManager::ConcurrentGetInodeAttr,
                                     this, &iStack, &stackMutex, &hardLinkMap,
                                     &mapMutex, &summaryInfo, &valueMutex,
                                     &inflightNum, &ret));
    } catch (const std::exception& e) {
      LOG(WARNING) << "CalAllLayerSumInfo create thread failed,"
                   << " err: " << e.what();
    }
  }

  if (threadpool.empty()) {
    return CURVEFS_ERROR::INTERNAL;
  }

  for (auto& thread : threadpool) {
    thread.join();
  }

  if (!ret.load()) {
    return CURVEFS_ERROR::INTERNAL;
  }

  // deal with hardlink
  for (const auto& it : hardLinkMap) {
    summaryInfo.fbytes -= it.second;
  }

  attr->mutable_xattr()->insert(
      {XATTR_DIR_RFILES, std::to_string(summaryInfo.files)});
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RSUBDIRS, std::to_string(summaryInfo.subdirs)});
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RENTRIES, std::to_string(summaryInfo.entries)});
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RFBYTES, std::to_string(summaryInfo.fbytes + attr->length())});
  return CURVEFS_ERROR::OK;
}

void XattrManager::ConcurrentGetInodeXattr(
    std::stack<uint64_t>* iStack, std::mutex* stackMutex, InodeAttr* attr,
    std::mutex* inodeMutex, Atomic<uint32_t>* inflightNum, Atomic<bool>* ret) {
  while (1) {
    std::list<Dentry> dentryList;
    std::set<uint64_t> inodeIds;
    std::list<XAttr> xattrs;
    auto tret = ConcurrentListDentry(&dentryList, iStack, stackMutex, true,
                                     inflightNum, ret);
    if (!tret) {
      return;
    }
    {
      std::lock_guard<std::mutex> guard(*stackMutex);
      for (const auto& it : dentryList) {
        iStack->emplace(it.inodeid());
        inodeIds.emplace(it.inodeid());
      }
      inflightNum->fetch_sub(1);
    }
    if (!inodeIds.empty()) {
      auto tret = inodeManager_->BatchGetXAttr(&inodeIds, &xattrs);
      if (tret == CURVEFS_ERROR::OK) {
        SummaryInfo summaryInfo;
        for (const auto& it : xattrs) {
          if (it.xattrinfos().count(XATTR_DIR_FILES)) {
            if (!AddUllStringToFirst(
                    &summaryInfo.files,
                    it.xattrinfos().find(XATTR_DIR_FILES)->second)) {
              ret->store(false);
              return;
            }
          }
          if (it.xattrinfos().count(XATTR_DIR_SUBDIRS)) {
            if (!AddUllStringToFirst(
                    &summaryInfo.subdirs,
                    it.xattrinfos().find(XATTR_DIR_SUBDIRS)->second)) {
              ret->store(false);
              return;
            }
          }
          if (it.xattrinfos().count(XATTR_DIR_ENTRIES)) {
            if (!AddUllStringToFirst(
                    &summaryInfo.entries,
                    it.xattrinfos().find(XATTR_DIR_ENTRIES)->second)) {
              ret->store(false);
              return;
            }
          }
          if (it.xattrinfos().count(XATTR_DIR_FBYTES)) {
            if (!AddUllStringToFirst(
                    &summaryInfo.fbytes,
                    it.xattrinfos().find(XATTR_DIR_FBYTES)->second)) {
              ret->store(false);
              return;
            }
          }
        }
        // record summary info to target inode
        std::lock_guard<std::mutex> guard(*inodeMutex);
        if (!(AddUllStringToFirst(
                  &(attr->mutable_xattr()->find(XATTR_DIR_RFILES)->second),
                  summaryInfo.files, true) &&
              AddUllStringToFirst(
                  &(attr->mutable_xattr()->find(XATTR_DIR_RSUBDIRS)->second),
                  summaryInfo.subdirs, true) &&
              AddUllStringToFirst(
                  &(attr->mutable_xattr()->find(XATTR_DIR_RENTRIES)->second),
                  summaryInfo.entries, true) &&
              AddUllStringToFirst(
                  &(attr->mutable_xattr()->find(XATTR_DIR_RFBYTES)->second),
                  summaryInfo.fbytes, true))) {
          ret->store(false);
          return;
        }
      } else {
        ret->store(false);
        return;
      }
    }
  }
}

CURVEFS_ERROR XattrManager::FastCalAllLayerSumInfo(InodeAttr* attr) {
  std::stack<uint64_t> iStack;
  std::mutex stackMutex;
  std::mutex inodeMutex;

  auto ino = attr->inodeid();
  iStack.emplace(ino);
  // add the size of itself first
  if (!AddUllStringToFirst(
          &(attr->mutable_xattr()->find(XATTR_DIR_FBYTES)->second),
          attr->length(), true)) {
    return CURVEFS_ERROR::INTERNAL;
  }

  // add first layer summary to all layer summary info
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RFILES, attr->xattr().find(XATTR_DIR_FILES)->second});
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RSUBDIRS, attr->xattr().find(XATTR_DIR_SUBDIRS)->second});
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RENTRIES, attr->xattr().find(XATTR_DIR_ENTRIES)->second});
  attr->mutable_xattr()->insert(
      {XATTR_DIR_RFBYTES, attr->xattr().find(XATTR_DIR_FBYTES)->second});

  std::vector<Thread> threadpool;
  Atomic<uint32_t> inflightNum(0);
  Atomic<bool> ret(true);
  for (auto i = listDentryThreads_; i > 0; i--) {
    try {
      threadpool.emplace_back(Thread(&XattrManager::ConcurrentGetInodeXattr,
                                     this, &iStack, &stackMutex, attr,
                                     &inodeMutex, &inflightNum, &ret));
    } catch (const std::exception& e) {
      LOG(WARNING) << "FastCalAllLayerSumInfo create thread failed,"
                   << " err: " << e.what();
    }
  }

  if (threadpool.empty()) {
    return CURVEFS_ERROR::INTERNAL;
  }

  for (auto& thread : threadpool) {
    thread.join();
  }

  if (!ret.load()) {
    return CURVEFS_ERROR::INTERNAL;
  }

  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR XattrManager::GetXattr(const char* name, std::string* value,
                                     InodeAttr* attr, bool enableSumInDir) {
  CURVEFS_ERROR ret = CURVEFS_ERROR::OK;
  // get summary info if the xattr name is summary type
  if (IsSummaryInfo(name) && attr->type() == FsFileType::TYPE_DIRECTORY) {
    // if not enable record summary info in dir xattr,
    // need recursive computation all files;
    // otherwise only recursive computation all dirs.
    if (!enableSumInDir) {
      if (IsOneLayer(name)) {
        ret = CalOneLayerSumInfo(attr);
      } else {
        ret = CalAllLayerSumInfo(attr);
      }
    } else {
      if (IsOneLayer(name)) {
        ret = FastCalOneLayerSumInfo(attr);
      } else {
        ret = FastCalAllLayerSumInfo(attr);
      }
    }

    if (CURVEFS_ERROR::OK != ret) {
      return ret;
    }
    LOG(INFO) << "After calculate summary info:\n" << attr->DebugString();
  }

  auto it = attr->xattr().find(name);
  if (it != attr->xattr().end()) {
    *value = it->second;
  }
  return ret;
}

CURVEFS_ERROR XattrManager::UpdateParentInodeXattr(uint64_t parentId,
                                                   const XAttr& xattr,
                                                   bool direction) {
  VLOG(6) << "UpdateParentInodeXattr inodeId = " << parentId
          << ", direction = " << direction
          << ", xattr = " << xattr.ShortDebugString();
  std::shared_ptr<InodeWrapper> pInodeWrapper;
  CURVEFS_ERROR ret = inodeManager_->GetInode(parentId, pInodeWrapper);
  if (ret != CURVEFS_ERROR::OK) {
    LOG(ERROR) << "UpdateParentInodeXattr get parent inode fail, ret = " << ret
               << ", inodeid = " << parentId;
    return ret;
  }

  ::curvefs::utils::UniqueLock lgGuard = pInodeWrapper->GetUniqueLock();
  auto inodeXAttr = pInodeWrapper->GetInodeLocked()->xattr();
  bool update = false;
  for (const auto& it : xattr.xattrinfos()) {
    auto iter = inodeXAttr.find(it.first);
    if (iter != inodeXAttr.end()) {
      uint64_t dat = 0;
      if (StringToUll(it.second, &dat)) {
        if (!AddUllStringToFirst(&(iter->second), dat, direction)) {
          return CURVEFS_ERROR::INTERNAL;
        }
      } else {
        LOG(ERROR) << "StringToUll failed, first = " << it.second;
        return CURVEFS_ERROR::INTERNAL;
      }
      update = true;
    }
  }

  if (update) {
    pInodeWrapper->MergeXAttrLocked(inodeXAttr);
    inodeManager_->ShipToFlush(pInodeWrapper);
  }

  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR XattrManager::UpdateParentXattrAfterRename(
    uint64_t parent, uint64_t newparent, const char* newname,
    RenameOperator* renameOp) {
  CURVEFS_ERROR rc = CURVEFS_ERROR::OK;
  if (parent != newparent) {
    Dentry dentry;
    rc = dentryManager_->GetDentry(newparent, newname, &dentry);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "dentryManager_ GetDentry fail, ret = " << rc
                 << ", parent = " << newparent << ", name = " << newname;
      return rc;
    }
    uint64_t ino = dentry.inodeid();

    std::shared_ptr<InodeWrapper> inodeWrapper;
    rc = inodeManager_->GetInode(ino, inodeWrapper);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "inodeManager get inode fail, ret = " << rc
                 << ", inodeid = " << ino;
      return rc;
    }
    XAttr xattr;
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_ENTRIES, "1"});
    if (dentry.type() == FsFileType::TYPE_DIRECTORY) {
      xattr.mutable_xattrinfos()->insert({XATTR_DIR_SUBDIRS, "1"});
    } else {
      xattr.mutable_xattrinfos()->insert({XATTR_DIR_FILES, "1"});
    }
    xattr.mutable_xattrinfos()->insert(
        {XATTR_DIR_FBYTES, std::to_string(inodeWrapper->GetLength())});

    // update src parent
    rc = UpdateParentInodeXattr(parent, xattr, false);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "UpdateParentInodeXattr failed, ret = " << rc
                 << "parentId = " << parent
                 << ", xattr = " << xattr.DebugString();
      return rc;
    }

    // update dest parent
    rc = UpdateParentInodeXattr(newparent, xattr, true);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "UpdateParentInodeXattr failed, ret = " << rc
                 << "parentId = " << newparent
                 << ", xattr = " << xattr.DebugString();
      return rc;
    }
  }

  // if rename dest exist and is file or empty dir, it will be overwirte
  uint64_t oldInode;
  int64_t oldInodeSize;
  FsFileType oldInodeType;
  renameOp->GetOldInode(&oldInode, &oldInodeSize, &oldInodeType);
  if (oldInode != 0 && oldInodeSize >= 0) {
    XAttr xattr;
    xattr.mutable_xattrinfos()->insert({XATTR_DIR_ENTRIES, "1"});
    if (oldInodeType == FsFileType::TYPE_DIRECTORY) {
      xattr.mutable_xattrinfos()->insert({XATTR_DIR_SUBDIRS, "1"});
    } else {
      xattr.mutable_xattrinfos()->insert({XATTR_DIR_FILES, "1"});
    }
    xattr.mutable_xattrinfos()->insert(
        {XATTR_DIR_FBYTES, std::to_string(oldInodeSize)});

    rc = UpdateParentInodeXattr(newparent, xattr, false);
    if (rc != CURVEFS_ERROR::OK) {
      LOG(ERROR) << "UpdateParentInodeXattr failed, ret = " << rc
                 << "parentId = " << newparent
                 << ", xattr = " << xattr.DebugString();
      return rc;
    }
  }

  return rc;
}

}  // namespace client
}  // namespace curvefs
