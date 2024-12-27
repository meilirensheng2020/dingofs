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

#ifndef DINGOFS_SRC_CLIENT_FUSE_CLIENT_H_
#define DINGOFS_SRC_CLIENT_FUSE_CLIENT_H_

#include <bthread/unstable.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>

#include "dingofs/proto/common.pb.h"
#include "dingofs/proto/mds.pb.h"
#include "dingofs/src/client/client_operator.h"
#include "dingofs/src/client/common/common.h"
#include "dingofs/src/client/common/config.h"
#include "dingofs/src/client/dentry_cache_manager.h"
#include "dingofs/src/client/filesystem/filesystem.h"
#include "dingofs/src/client/filesystem/meta.h"
#include "dingofs/src/client/fuse_common.h"
#include "dingofs/src/client/inode_cache_manager.h"
#include "dingofs/src/client/lease/lease_excutor.h"
#include "dingofs/src/client/s3/client_s3_adaptor.h"
#include "dingofs/src/client/warmup/warmup_manager.h"
#include "dingofs/src/client/xattr_manager.h"
#include "dingofs/src/stub/metric/metric.h"
#include "dingofs/src/stub/rpcclient/mds_client.h"
#include "dingofs/src/stub/rpcclient/metaserver_client.h"
#include "dingofs/src/utils/concurrent/concurrent.h"
#include "dingofs/src/utils/fast_align.h"
#include "dingofs/src/utils/throttle.h"

#define PORT_LIMIT 65535

#define DirectIOAlignment 512

namespace dingofs {
namespace client {

namespace warmup {
class WarmupManager;
}

using common::FuseClientOption;
using ::dingofs::client::filesystem::AttrOut;
using ::dingofs::client::filesystem::EntryOut;
using ::dingofs::client::filesystem::FileOut;
using ::dingofs::client::filesystem::FileSystem;
using ::dingofs::common::FSType;
using ::dingofs::metaserver::DentryFlag;
using ::dingofs::metaserver::ManageInodeType;
using ::dingofs::utils::Atomic;
using ::dingofs::utils::InterruptibleSleeper;
using ::dingofs::utils::Thread;
using ::dingofs::utils::Throttle;

using ::dingofs::stub::metric::FSMetric;
using dingofs::stub::rpcclient::MDSBaseClient;
using dingofs::stub::rpcclient::MdsClient;
using dingofs::stub::rpcclient::MdsClientImpl;
using dingofs::stub::rpcclient::MetaServerClient;
using dingofs::stub::rpcclient::MetaServerClientImpl;

using dingofs::utils::is_aligned;

const uint32_t kMaxHostNameLength = 255u;

using mds::Mountpoint;

class FuseClient {
 public:
  FuseClient()
      : mdsClient_(std::make_shared<MdsClientImpl>()),
        metaClient_(std::make_shared<MetaServerClientImpl>()),
        inodeManager_(std::make_shared<InodeCacheManagerImpl>(metaClient_)),
        dentryManager_(std::make_shared<DentryCacheManagerImpl>(metaClient_)),
        fsInfo_(nullptr),
        init_(false),
        enableSumInDir_(false),
        warmupManager_(nullptr),
        mdsBase_(nullptr),
        isStop_(true) {}

  virtual ~FuseClient() = default;

  FuseClient(const std::shared_ptr<MdsClient>& mdsClient,
             const std::shared_ptr<MetaServerClient>& metaClient,
             const std::shared_ptr<InodeCacheManager>& inodeManager,
             const std::shared_ptr<DentryCacheManager>& dentryManager,
             const std::shared_ptr<warmup::WarmupManager>& warmupManager)
      : mdsClient_(mdsClient),
        metaClient_(metaClient),
        inodeManager_(inodeManager),
        dentryManager_(dentryManager),
        fsInfo_(nullptr),
        init_(false),
        enableSumInDir_(false),
        warmupManager_(warmupManager),
        mdsBase_(nullptr),
        isStop_(true) {}

  virtual DINGOFS_ERROR Init(const FuseClientOption& option);

  virtual void UnInit();

  virtual DINGOFS_ERROR Run();

  virtual void Fini();

  virtual DINGOFS_ERROR FuseOpInit(void* userdata, struct fuse_conn_info* conn);

  virtual void FuseOpDestroy(void* userdata);

  virtual DINGOFS_ERROR FuseOpWrite(fuse_req_t req, fuse_ino_t ino,
                                    const char* buf, size_t size, off_t off,
                                    struct fuse_file_info* fi,
                                    FileOut* file_out) = 0;

  virtual DINGOFS_ERROR FuseOpRead(fuse_req_t req, fuse_ino_t ino, size_t size,
                                   off_t off, struct fuse_file_info* fi,
                                   char* buffer, size_t* rSize) = 0;

  virtual DINGOFS_ERROR FuseOpLookup(fuse_req_t req, fuse_ino_t parent,
                                     const char* name, EntryOut* entryOut);

  virtual DINGOFS_ERROR FuseOpOpen(fuse_req_t req, fuse_ino_t ino,
                                   struct fuse_file_info* fi, FileOut* fileOut);

  virtual DINGOFS_ERROR FuseOpCreate(fuse_req_t req, fuse_ino_t parent,
                                     const char* name, mode_t mode,
                                     struct fuse_file_info* fi,
                                     EntryOut* entryOut) = 0;

  virtual DINGOFS_ERROR FuseOpMkNod(fuse_req_t req, fuse_ino_t parent,
                                    const char* name, mode_t mode, dev_t rdev,
                                    EntryOut* entryOut) = 0;

  virtual DINGOFS_ERROR FuseOpMkDir(fuse_req_t req, fuse_ino_t parent,
                                    const char* name, mode_t mode,
                                    EntryOut* entryOut);

  virtual DINGOFS_ERROR FuseOpUnlink(fuse_req_t req, fuse_ino_t parent,
                                     const char* name) = 0;

  virtual DINGOFS_ERROR FuseOpRmDir(fuse_req_t req, fuse_ino_t parent,
                                    const char* name);

  virtual DINGOFS_ERROR FuseOpOpenDir(fuse_req_t req, fuse_ino_t ino,
                                      struct fuse_file_info* fi);

  virtual DINGOFS_ERROR FuseOpReleaseDir(fuse_req_t req, fuse_ino_t ino,
                                         struct fuse_file_info* fi);

  virtual DINGOFS_ERROR FuseOpReadDir(fuse_req_t req, fuse_ino_t ino,
                                      size_t size, off_t off,
                                      struct fuse_file_info* fi,
                                      char** bufferOut, size_t* rSize,
                                      bool plus);

  virtual DINGOFS_ERROR FuseOpRename(fuse_req_t req, fuse_ino_t parent,
                                     const char* name, fuse_ino_t newparent,
                                     const char* newname, unsigned int flags);

  virtual DINGOFS_ERROR FuseOpGetAttr(fuse_req_t req, fuse_ino_t ino,
                                      struct fuse_file_info* fi,
                                      struct AttrOut* out);

  virtual DINGOFS_ERROR FuseOpSetAttr(fuse_req_t req, fuse_ino_t ino,
                                      struct stat* attr, int to_set,
                                      struct fuse_file_info* fi,
                                      struct AttrOut* attr_out);

  virtual DINGOFS_ERROR FuseOpGetXattr(fuse_req_t req, fuse_ino_t ino,
                                       const char* name, std::string* value,
                                       size_t size);

  virtual DINGOFS_ERROR FuseOpSetXattr(fuse_req_t req, fuse_ino_t ino,
                                       const char* name, const char* value,
                                       size_t size, int flags);

  virtual DINGOFS_ERROR FuseOpListXattr(fuse_req_t req, fuse_ino_t ino,
                                        char* value, size_t size,
                                        size_t* realSize);

  virtual DINGOFS_ERROR FuseOpSymlink(fuse_req_t req, const char* link,
                                      fuse_ino_t parent, const char* name,
                                      EntryOut* entry_out);

  virtual DINGOFS_ERROR FuseOpLink(fuse_req_t req, fuse_ino_t ino,
                                   fuse_ino_t newparent, const char* newname,
                                   EntryOut* entryOut) = 0;

  virtual DINGOFS_ERROR FuseOpReadLink(fuse_req_t req, fuse_ino_t ino,
                                       std::string* linkStr);

  virtual DINGOFS_ERROR FuseOpRelease(fuse_req_t req, fuse_ino_t ino,
                                      struct fuse_file_info* fi);

  virtual DINGOFS_ERROR FuseOpFsync(fuse_req_t req, fuse_ino_t ino,
                                    int datasync,
                                    struct fuse_file_info* fi) = 0;
  virtual DINGOFS_ERROR FuseOpFlush(fuse_req_t req, fuse_ino_t ino,
                                    struct fuse_file_info* fi) {
    (void)req;
    (void)ino;
    (void)fi;
    return DINGOFS_ERROR::OK;
  }

  virtual DINGOFS_ERROR FuseOpStatFs(fuse_req_t req, fuse_ino_t ino,
                                     struct statvfs* stbuf);

  virtual DINGOFS_ERROR Truncate(InodeWrapper* inode, uint64_t length) = 0;

  void SetFsInfo(const std::shared_ptr<FsInfo>& fsInfo) {
    fsInfo_ = fsInfo;
    init_ = true;
  }

  void SetMounted(bool mounted) {
    if (warmupManager_ != nullptr) {
      warmupManager_->SetMounted(mounted);
    }
  }

  std::shared_ptr<FsInfo> GetFsInfo() { return fsInfo_; }

  std::shared_ptr<FileSystem> GetFileSystem() { return fs_; }

  virtual void FlushAll();

  // for unit test
  void SetEnableSumInDir(bool enable) { enableSumInDir_ = enable; }

  bool PutWarmFilelistTask(fuse_ino_t key, common::WarmupStorageType type) {
    if (fsInfo_->fstype() == FSType::TYPE_S3) {
      return warmupManager_->AddWarmupFilelist(key, type);
    }  // only support s3
    return true;
  }

  bool PutWarmFileTask(fuse_ino_t key, const std::string& path,
                       common::WarmupStorageType type) {
    if (fsInfo_->fstype() == FSType::TYPE_S3) {
      return warmupManager_->AddWarmupFile(key, path, type);
    }  // only support s3
    return true;
  }

  bool GetWarmupProgress(fuse_ino_t key, warmup::WarmupProgress* progress) {
    if (fsInfo_->fstype() == FSType::TYPE_S3) {
      return warmupManager_->QueryWarmupProgress(key, progress);
    }
    return false;
  }

  DINGOFS_ERROR SetMountStatus(const struct MountOption* mountOption);

  void Add(bool isRead, size_t size) { throttle_.Add(isRead, size); }

  void InitQosParam();

 protected:
  DINGOFS_ERROR MakeNode(fuse_req_t req, fuse_ino_t parent, const char* name,
                         mode_t mode, FsFileType type, dev_t rdev,
                         bool internal,
                         std::shared_ptr<InodeWrapper>& inode_wrapper);

  DINGOFS_ERROR OpUnlink(fuse_req_t req, fuse_ino_t parent, const char* name,
                         FsFileType type);

  DINGOFS_ERROR OpLink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                       const char* newname, FsFileType type,
                       EntryOut* entry_out);

  DINGOFS_ERROR CreateManageNode(fuse_req_t req, uint64_t parent,
                                 const char* name, mode_t mode,
                                 ManageInodeType manageType,
                                 EntryOut* entryOut);

  DINGOFS_ERROR GetOrCreateRecycleDir(fuse_req_t req, Dentry* out);

  DINGOFS_ERROR MoveToRecycle(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent,
                              const char* name, FsFileType type);

  bool ShouldMoveToRecycle(fuse_ino_t parent);

  DINGOFS_ERROR HandleOpenFlags(fuse_req_t req, fuse_ino_t ino,
                                struct fuse_file_info* fi, FileOut* fileOut);

  int SetHostPortInMountPoint(Mountpoint* out) {
    char hostname[kMaxHostNameLength];
    int ret = gethostname(hostname, kMaxHostNameLength);
    if (ret < 0) {
      LOG(ERROR) << "GetHostName failed, ret = " << ret;
      return ret;
    }
    out->set_hostname(hostname);
    out->set_port(
        dingofs::stub::common::ClientDummyServerInfo::GetInstance().GetPort());
    return 0;
  }

  virtual DINGOFS_ERROR InitBrpcServer();

 private:
  virtual void FlushData() = 0;

  DINGOFS_ERROR UpdateParentMCTimeAndNlink(fuse_ino_t parent, FsFileType type,
                                           NlinkChange nlink);

  std::string GenerateNewRecycleName(fuse_ino_t ino, fuse_ino_t parent,
                                     const char* name) {
    std::string newName(name);
    newName =
        std::to_string(parent) + "_" + std::to_string(ino) + "_" + newName;
    if (newName.length() > option_.fileSystemOption.maxNameLength) {
      newName = newName.substr(0, option_.fileSystemOption.maxNameLength);
    }

    return newName;
  }

  InodeAttr GenerateVirtualInodeAttr(fuse_ino_t ino, uint32_t fsid) {
    InodeAttr attr;

    attr.set_inodeid(ino);
    attr.set_fsid(fsid);
    attr.set_nlink(1);
    attr.set_mode(S_IFREG | 0444);
    attr.set_type(FsFileType::TYPE_S3);
    // attr.set_uid(0);
    // attr.set_gid(0);
    attr.set_length(0);
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    attr.set_mtime(now.tv_sec);
    attr.set_mtime_ns(now.tv_nsec);
    attr.set_atime(now.tv_sec);
    attr.set_atime_ns(now.tv_nsec);
    attr.set_ctime(now.tv_sec);
    attr.set_ctime_ns(now.tv_nsec);

    return attr;
  }

 protected:
  // mds client
  std::shared_ptr<MdsClient> mdsClient_;

  // metaserver client
  std::shared_ptr<MetaServerClient> metaClient_;

  // inode cache manager
  std::shared_ptr<InodeCacheManager> inodeManager_;

  // dentry cache manager
  std::shared_ptr<DentryCacheManager> dentryManager_;

  // xattr manager
  std::shared_ptr<XattrManager> xattrManager_;

  std::shared_ptr<LeaseExecutor> leaseExecutor_;

  // filesystem info
  std::shared_ptr<FsInfo> fsInfo_;

  FuseClientOption option_;

  // init flags
  bool init_;

  // enable record summary info in dir inode xattr
  std::atomic<bool> enableSumInDir_;

  std::shared_ptr<FSMetric> fsMetric_;

  Mountpoint mountpoint_;

  // warmup manager
  std::shared_ptr<warmup::WarmupManager> warmupManager_;

  std::shared_ptr<FileSystem> fs_;

 private:
  MDSBaseClient* mdsBase_;

  Atomic<bool> isStop_;

  dingofs::utils::Mutex renameMutex_;

  Throttle throttle_;

  bthread_timer_t throttleTimer_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_CLIENT_H_
