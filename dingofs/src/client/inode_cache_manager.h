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

#ifndef DINGOFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
#define DINGOFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <set>

#include "dingofs/proto/metaserver.pb.h"
#include "dingofs/src/client/common/config.h"
#include "dingofs/src/client/filesystem/defer_sync.h"
#include "dingofs/src/client/filesystem/error.h"
#include "dingofs/src/client/filesystem/openfile.h"
#include "dingofs/src/client/inode_wrapper.h"
#include "dingofs/src/stub/rpcclient/metaserver_client.h"
#include "dingofs/src/stub/rpcclient/task_excutor.h"
#include "dingofs/src/utils/concurrent/concurrent.h"
#include "dingofs/src/utils/concurrent/generic_name_lock.h"

namespace dingofs {
namespace client {

using common::RefreshDataOption;
using ::dingofs::client::filesystem::DeferSync;
using ::dingofs::client::filesystem::OpenFiles;
using ::dingofs::metaserver::InodeAttr;
using ::dingofs::metaserver::XAttr;
using dingofs::utils::CountDownEvent;

using dingofs::stub::metric::S3ChunkInfoMetric;
using dingofs::stub::rpcclient::BatchGetInodeAttrDone;
using dingofs::stub::rpcclient::InodeParam;
using dingofs::stub::rpcclient::MetaServerClient;
using dingofs::stub::rpcclient::MetaServerClientImpl;

class InodeCacheManager {
 public:
  InodeCacheManager() : m_fs_id(0) {}
  virtual ~InodeCacheManager() = default;

  void SetFsId(uint32_t fs_id) { m_fs_id = fs_id; }

  virtual DINGOFS_ERROR Init(RefreshDataOption option,
                             std::shared_ptr<OpenFiles> open_files,
                             std::shared_ptr<DeferSync> defer_sync) = 0;

  virtual DINGOFS_ERROR GetInode(
      uint64_t inode_id,
      std::shared_ptr<InodeWrapper>& out) = 0;  // NOLINT

  virtual DINGOFS_ERROR GetInodeAttr(uint64_t inode_id, InodeAttr* out) = 0;

  virtual DINGOFS_ERROR BatchGetInodeAttr(std::set<uint64_t>* inode_ids,
                                          std::list<InodeAttr>* attrs) = 0;

  virtual DINGOFS_ERROR BatchGetInodeAttrAsync(
      uint64_t parent_id, std::set<uint64_t>* inode_ids,
      std::map<uint64_t, InodeAttr>* attrs) = 0;

  virtual DINGOFS_ERROR BatchGetXAttr(std::set<uint64_t>* inode_ids,
                                      std::list<XAttr>* xattrs) = 0;

  virtual DINGOFS_ERROR CreateInode(
      const InodeParam& param,
      std::shared_ptr<InodeWrapper>& out) = 0;  // NOLINT

  virtual DINGOFS_ERROR CreateManageInode(
      const InodeParam& param,
      std::shared_ptr<InodeWrapper>& out) = 0;  // NOLINT

  virtual DINGOFS_ERROR DeleteInode(uint64_t inode_id) = 0;

  virtual void ShipToFlush(
      const std::shared_ptr<InodeWrapper>& inode_wrapper) = 0;

 protected:
  uint32_t m_fs_id;
};

class InodeCacheManagerImpl
    : public InodeCacheManager,
      public std::enable_shared_from_this<InodeCacheManagerImpl> {
 public:
  InodeCacheManagerImpl()
      : metaClient_(std::make_shared<MetaServerClientImpl>()) {}

  explicit InodeCacheManagerImpl(
      const std::shared_ptr<MetaServerClient>& meta_client)
      : metaClient_(meta_client) {}

  DINGOFS_ERROR Init(RefreshDataOption option,
                     std::shared_ptr<OpenFiles> open_files,
                     std::shared_ptr<DeferSync> defer_sync) override {
    option_ = option;
    s3ChunkInfoMetric_ = std::make_shared<S3ChunkInfoMetric>();
    openFiles_ = open_files;
    deferSync_ = defer_sync;
    return DINGOFS_ERROR::OK;
  }

  DINGOFS_ERROR GetInode(uint64_t inode_id,
                         std::shared_ptr<InodeWrapper>& out) override;

  DINGOFS_ERROR GetInodeAttr(uint64_t inode_id, InodeAttr* out) override;

  DINGOFS_ERROR BatchGetInodeAttr(std::set<uint64_t>* inode_ids,
                                  std::list<InodeAttr>* attrs) override;

  DINGOFS_ERROR BatchGetInodeAttrAsync(
      uint64_t parent_id, std::set<uint64_t>* inode_ids,
      std::map<uint64_t, InodeAttr>* attrs = nullptr) override;

  DINGOFS_ERROR BatchGetXAttr(std::set<uint64_t>* inode_ids,
                              std::list<XAttr>* xattrs) override;

  DINGOFS_ERROR CreateInode(const InodeParam& param,
                            std::shared_ptr<InodeWrapper>& out) override;

  DINGOFS_ERROR CreateManageInode(const InodeParam& param,
                                  std::shared_ptr<InodeWrapper>& out) override;

  DINGOFS_ERROR DeleteInode(uint64_t inode_id) override;

  void ShipToFlush(const std::shared_ptr<InodeWrapper>& inode_wrapper) override;

 private:
  DINGOFS_ERROR GetInodeFromCached(uint64_t inode_id,
                                   std::shared_ptr<InodeWrapper>& out);

  DINGOFS_ERROR GetInodeFromCachedUnlocked(uint64_t inode_id,
                                           std::shared_ptr<InodeWrapper>& out);

  static DINGOFS_ERROR RefreshData(std::shared_ptr<InodeWrapper>& inode,
                                   bool streaming = true);

  std::shared_ptr<MetaServerClient> metaClient_;
  std::shared_ptr<S3ChunkInfoMetric> s3ChunkInfoMetric_;

  std::shared_ptr<OpenFiles> openFiles_;

  std::shared_ptr<DeferSync> deferSync_;

  dingofs::utils::GenericNameLock<Mutex> nameLock_;

  dingofs::utils::GenericNameLock<Mutex> asyncNameLock_;

  RefreshDataOption option_;
};

class BatchGetInodeAttrAsyncDone : public BatchGetInodeAttrDone {
 public:
  BatchGetInodeAttrAsyncDone(std::map<uint64_t, InodeAttr>* attrs,
                             ::dingofs::utils::Mutex* mutex,
                             std::shared_ptr<CountDownEvent> cond)
      : mutex_(mutex), attrs_(attrs), cond_(cond) {}

  ~BatchGetInodeAttrAsyncDone() override = default;

  void Run() override {
    std::unique_ptr<BatchGetInodeAttrAsyncDone> self_guard(this);
    MetaStatusCode ret = GetStatusCode();
    if (ret != MetaStatusCode::OK) {
      LOG(ERROR) << "BatchGetInodeAttrAsync failed, "
                 << ", MetaStatusCode: " << ret
                 << ", MetaStatusCode_Name: " << MetaStatusCode_Name(ret)
                 << ", size: " << GetInodeAttrs().size();
    } else {
      auto inode_attrs = GetInodeAttrs();
      VLOG(3) << "BatchGetInodeAttrAsyncDone update inodeAttrCache"
              << " size: " << inode_attrs.size();

      dingofs::utils::LockGuard lk(*mutex_);
      for (const auto& attr : inode_attrs) {
        attrs_->emplace(attr.inodeid(), attr);
      }
    }
    cond_->Signal();
  };

 private:
  ::dingofs::utils::Mutex* mutex_;
  std::map<uint64_t, InodeAttr>* attrs_;
  std::shared_ptr<CountDownEvent> cond_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_INODE_CACHE_MANAGER_H_
