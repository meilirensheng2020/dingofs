// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDV2_FILESYSTEM_H_
#define DINGOFS_MDV2_FILESYSTEM_H_

#include <sys/types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/context.h"
#include "mdsv2/common/status.h"
#include "mdsv2/common/type.h"
#include "mdsv2/filesystem/chunk_cache.h"
#include "mdsv2/filesystem/dentry.h"
#include "mdsv2/filesystem/file_session.h"
#include "mdsv2/filesystem/fs_info.h"
#include "mdsv2/filesystem/id_generator.h"
#include "mdsv2/filesystem/inode.h"
#include "mdsv2/filesystem/notify_buddy.h"
#include "mdsv2/filesystem/parent_memo.h"
#include "mdsv2/filesystem/partition.h"
#include "mdsv2/filesystem/renamer.h"
#include "mdsv2/filesystem/store_operation.h"
#include "mdsv2/mds/mds_meta.h"
#include "mdsv2/quota/quota.h"
#include "mdsv2/storage/storage.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mdsv2 {

class FileSystem;
using FileSystemSPtr = std::shared_ptr<FileSystem>;

class FileSystemSet;
using FileSystemSetSPtr = std::shared_ptr<FileSystemSet>;

class GcProcessor;
using GcProcessorSPtr = std::shared_ptr<GcProcessor>;

struct EntryOut {
  EntryOut() = default;
  using AttrType = Inode::AttrType;

  explicit EntryOut(const AttrType& attr) : attr(attr) {}

  std::string name;
  AttrType attr;
  uint64_t parent_version{0};
};

class FileSystem : public std::enable_shared_from_this<FileSystem> {
 public:
  FileSystem(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorUPtr id_generator, IdGeneratorSPtr slice_id_generator,
             KVStorageSPtr kv_storage, OperationProcessorSPtr operation_processor, MDSMetaMapSPtr mds_meta_map,
             notify::NotifyBuddySPtr notify_buddy);
  ~FileSystem();

  FileSystem(const FileSystem&) = delete;
  FileSystem& operator=(const FileSystem&) = delete;
  FileSystem(FileSystem&&) = delete;
  FileSystem& operator=(FileSystem&&) = delete;

  static FileSystemSPtr New(int64_t self_mds_id, FsInfoUPtr fs_info, IdGeneratorUPtr id_generator,
                            IdGeneratorSPtr slice_id_generator, KVStorageSPtr kv_storage,
                            OperationProcessorSPtr operation_processor, MDSMetaMapSPtr mds_meta_map,
                            notify::NotifyBuddySPtr notify_buddy) {
    return std::make_shared<FileSystem>(self_mds_id, std::move(fs_info), std::move(id_generator), slice_id_generator,
                                        kv_storage, operation_processor, mds_meta_map, notify_buddy);
  }

  FileSystemSPtr GetSelfPtr();

  bool Init();

  uint32_t FsId() const { return fs_id_; }
  std::string FsName() const { return fs_info_->GetName(); }

  uint64_t Epoch() const;

  FsInfoType GetFsInfo() const { return fs_info_->Get(); }

  pb::mdsv2::PartitionType PartitionType() const;
  bool IsMonoPartition() const;
  bool IsParentHashPartition() const;

  bool CanServe() const { return can_serve_; };

  // create root directory
  Status CreateRoot();

  Status CreateQuota();

  // lookup dentry
  Status Lookup(Context& ctx, Ino parent, const std::string& name, EntryOut& entry_out);

  // file
  struct MkNodParam {
    std::string name;
    uint32_t flag{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t mode{0};
    Ino parent{0};
    uint64_t rdev{0};
  };
  Status MkNod(Context& ctx, const MkNodParam& param, EntryOut& entry_out);
  Status Open(Context& ctx, Ino ino, uint32_t flags, std::string& session_id, uint64_t& version);
  Status Release(Context& ctx, Ino ino, const std::string& session_id);

  // directory
  struct MkDirParam {
    std::string name;
    uint32_t flag{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t mode{0};
    Ino parent{0};
    uint64_t rdev{0};
  };
  Status MkDir(Context& ctx, const MkDirParam& param, EntryOut& entry_out);
  Status RmDir(Context& ctx, Ino parent, const std::string& name);
  Status ReadDir(Context& ctx, Ino ino, const std::string& last_name, uint limit, bool with_attr,
                 std::vector<EntryOut>& entry_outs);

  // create hard link
  Status Link(Context& ctx, Ino ino, Ino new_parent, const std::string& new_name, EntryOut& entry_out);
  // delete link
  Status UnLink(Context& ctx, Ino parent, const std::string& name);
  // create symbolic link
  Status Symlink(Context& ctx, const std::string& symlink, Ino new_parent, const std::string& new_name, uint32_t uid,
                 uint32_t gid, EntryOut& entry_out);
  // read symbolic link
  Status ReadLink(Context& ctx, Ino ino, std::string& link);

  // attr
  struct SetAttrParam {
    uint32_t to_set{0};
    AttrType attr;
  };

  Status SetAttr(Context& ctx, Ino ino, const SetAttrParam& param, EntryOut& entry_out);
  Status GetAttr(Context& ctx, Ino ino, EntryOut& entry_out);

  // xattr
  Status GetXAttr(Context& ctx, Ino ino, Inode::XAttrMap& xattr);
  Status GetXAttr(Context& ctx, Ino ino, const std::string& name, std::string& value);
  Status SetXAttr(Context& ctx, Ino ino, const Inode::XAttrMap& xattrs, uint64_t& version);
  Status RemoveXAttr(Context& ctx, Ino ino, const std::string& name, uint64_t& version);

  // rename
  struct RenameParam {
    Ino old_parent{0};
    std::string old_name;
    Ino new_parent{0};
    std::string new_name;

    std::vector<Ino> old_ancestors;
    std::vector<Ino> new_ancestors;
  };
  Status Rename(Context& ctx, const RenameParam& param, uint64_t& old_parent_version, uint64_t& new_parent_version);
  Status CommitRename(Context& ctx, const RenameParam& param, uint64_t& old_parent_version,
                      uint64_t& new_parent_version);

  // slice
  Status WriteSlice(Context& ctx, Ino parent, Ino ino, uint64_t chunk_index,
                    const std::vector<pb::mdsv2::Slice>& slices);
  Status ReadSlice(Context& ctx, Ino ino, uint64_t chunk_index, std::vector<pb::mdsv2::Slice>& slices);

  // fallocate
  Status Fallocate(Context& ctx, Ino ino, int32_t mode, uint64_t offset, uint64_t len, EntryOut& entry_out);

  // compact
  Status CompactChunk(Context& ctx, Ino ino, uint64_t chunk_index, std::vector<pb::mdsv2::TrashSlice>& trash_slices);

  // dentry/inode
  Status GetDentry(Context& ctx, Ino parent, const std::string& name, Dentry& dentry);
  Status ListDentry(Context& ctx, Ino parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                    std::vector<Dentry>& dentries);
  Status GetInode(Context& ctx, Ino ino, EntryOut& entry_out);
  Status BatchGetInode(Context& ctx, const std::vector<uint64_t>& inoes, std::vector<EntryOut>& out_entries);
  Status BatchGetXAttr(Context& ctx, const std::vector<uint64_t>& inoes, std::vector<pb::mdsv2::XAttr>& out_xattrs);

  Status RefreshInode(const std::vector<uint64_t>& inoes);
  void RefreshInode(AttrType& attr);

  Status RefreshFsInfo();
  Status RefreshFsInfo(const std::string& name);
  void RefreshFsInfo(const FsInfoType& fs_info);

  Status UpdatePartitionPolicy(uint64_t mds_id);
  Status UpdatePartitionPolicy(const std::map<uint64_t, pb::mdsv2::HashPartition::BucketSet>& distributions);

  PartitionCache& GetPartitionCache() { return partition_cache_; }
  InodeCache& GetInodeCache() { return inode_cache_; }

  quota::QuotaManager& GetQuotaManager() { return quota_manager_; }

  FileSessionManager& GetFileSessionManager() { return file_session_manager_; }

  // get deleted file
  Status GetDelFiles(std::vector<AttrType>& delfiles);
  Status GetDelSlices(std::vector<TrashSliceList>& delslices);

 private:
  friend class DebugServiceImpl;
  friend class FsStatServiceImpl;

  IdGenerator& GetInoIdGenerator() { return *ino_id_generator_; }

  Status RunOperation(Operation* operation);

  // generate ino
  Status GenDirIno(Ino& ino);
  Status GenFileIno(Ino& ino);
  bool CanServe(int64_t self_mds_id);

  // get partition
  Status GetPartition(Context& ctx, Ino parent, PartitionPtr& out_partition);
  Status GetPartition(Context& ctx, uint64_t version, Ino parent, PartitionPtr& out_partition);
  PartitionPtr GetPartitionFromCache(Ino parent);
  std::map<uint64_t, PartitionPtr> GetAllPartitionsFromCache();
  Status GetPartitionFromStore(Ino parent, const std::string& reason, PartitionPtr& out_partition);

  // get dentry
  Status GetDentryFromStore(Ino parent, const std::string& name, Dentry& dentry);
  Status ListDentryFromStore(Ino parent, const std::string& last_name, uint32_t limit, bool is_only_dir,
                             std::vector<Dentry>& dentries);

  // get inode
  Status GetInode(Context& ctx, Dentry& dentry, PartitionPtr partition, InodeSPtr& out_inode);
  Status GetInode(Context& ctx, uint64_t version, Dentry& dentry, PartitionPtr partition, InodeSPtr& out_inode);
  Status GetInode(Context& ctx, Ino ino, InodeSPtr& out_inode);
  Status GetInode(Context& ctx, uint64_t version, Ino ino, InodeSPtr& out_inode);
  InodeSPtr GetInodeFromCache(Ino ino);
  std::map<uint64_t, InodeSPtr> GetAllInodesFromCache();
  Status GetInodeFromStore(Ino ino, const std::string& reason, bool is_cache, InodeSPtr& out_inode);
  Status BatchGetInodeFromStore(std::vector<uint64_t> inoes, std::vector<InodeSPtr>& out_inodes);

  Status GetDelFileFromStore(Ino ino, AttrType& out_attr);

  // delete inode from cache
  void DeleteInodeFromCache(Ino ino);

  // thorough delete inode
  Status DestoryInode(uint32_t fs_id, Ino ino);

  uint64_t GetMdsIdByIno(Ino ino);

  void UpdateParentMemo(const std::vector<Ino>& ancestors);

  void NotifyBuddyRefreshInode(AttrType&& attr);
  void NotifyBuddyCleanPartitionCache(Ino ino);

  uint64_t self_mds_id_;

  // filesystem info
  FsInfoUPtr fs_info_;
  const uint32_t fs_id_;

  bool can_serve_{false};

  // generate inode id
  IdGeneratorUPtr ino_id_generator_;
  // for slice id
  IdGeneratorSPtr slice_id_generator_;

  // persistence store dentry/inode
  KVStorageSPtr kv_storage_;

  // for open/read/write/close file
  FileSessionManager file_session_manager_;

  // organize dentry directory tree
  PartitionCache partition_cache_;

  // organize inode
  InodeCache inode_cache_;

  // mds meta map
  MDSMetaMapSPtr mds_meta_map_;

  // parent memo
  ParentMemoSPtr parent_memo_;

  // chunk cache
  ChunkCache chunk_cache_;

  // quota
  quota::QuotaManager quota_manager_;

  // renamer
  Renamer renamer_;

  OperationProcessorSPtr operation_processor_;

  // notify buddy
  notify::NotifyBuddySPtr notify_buddy_;
};

// manage all filesystem
class FileSystemSet {
 public:
  FileSystemSet(CoordinatorClientSPtr coordinator_client, IdGeneratorUPtr fs_id_generator,
                IdGeneratorSPtr slice_id_generator, KVStorageSPtr kv_storage, MDSMeta self_mds_meta,
                MDSMetaMapSPtr mds_meta_map, OperationProcessorSPtr operation_processor,
                notify::NotifyBuddySPtr notify_buddy);
  ~FileSystemSet();

  FileSystemSet(const FileSystemSet&) = delete;
  FileSystemSet& operator=(const FileSystemSet&) = delete;
  FileSystemSet(FileSystemSet&&) = delete;
  FileSystemSet& operator=(FileSystemSet&&) = delete;

  static FileSystemSetSPtr New(CoordinatorClientSPtr coordinator_client, IdGeneratorUPtr fs_id_generator,
                               IdGeneratorSPtr slice_id_generator, KVStorageSPtr kv_storage, MDSMeta self_mds_meta,
                               MDSMetaMapSPtr mds_meta_map, OperationProcessorSPtr operation_processor,
                               notify::NotifyBuddySPtr notify_buddy) {
    return std::make_shared<FileSystemSet>(coordinator_client, std::move(fs_id_generator),
                                           std::move(slice_id_generator), kv_storage, self_mds_meta, mds_meta_map,
                                           operation_processor, notify_buddy);
  }

  bool Init();

  struct CreateFsParam {
    int64_t mds_id;
    std::string fs_name;
    uint64_t block_size;
    uint64_t chunk_size;
    pb::mdsv2::FsType fs_type;
    pb::mdsv2::FsExtra fs_extra;
    bool enable_sum_in_dir;
    std::string owner;
    uint64_t capacity;
    uint32_t recycle_time_hour;
    pb::mdsv2::PartitionType partition_type;
  };

  Status CreateFs(const CreateFsParam& param, FsInfoType& fs_info);
  Status MountFs(Context& ctx, const std::string& fs_name, const pb::mdsv2::MountPoint& mountpoint);
  Status UmountFs(Context& ctx, const std::string& fs_name, const std::string& client_id);
  Status DeleteFs(Context& ctx, const std::string& fs_name, bool is_force);
  Status UpdateFsInfo(Context& ctx, const std::string& fs_name, const FsInfoType& fs_info);
  Status GetFsInfo(Context& ctx, const std::string& fs_name, FsInfoType& fs_info);
  Status GetAllFsInfo(Context& ctx, std::vector<FsInfoType>& fs_infoes);
  Status RefreshFsInfo(const std::string& fs_name);
  Status RefreshFsInfo(uint32_t fs_id);

  Status AllocSliceId(uint32_t num, uint64_t min_slice_id, uint64_t& slice_id);

  FileSystemSPtr GetFileSystem(uint32_t fs_id);
  FileSystemSPtr GetFileSystem(const std::string& fs_name);
  uint32_t GetFsId(const std::string& fs_name);
  std::string GetFsName(const std::string& client_id);
  std::vector<FileSystemSPtr> GetAllFileSystem();

  // load already exist filesystem
  bool LoadFileSystems();

 private:
  friend class FsStatServiceImpl;

  IdGenerator& GetFsIdGenerator() { return *fs_id_generator_; }
  IdGenerator& GetSliceIdGenerator() { return *slice_id_generator_; }

  Status GenFsId(uint32_t& fs_id);
  FsInfoType GenFsInfo(int64_t fs_id, const CreateFsParam& param);

  bool IsExistFsTable();

  bool AddFileSystem(FileSystemSPtr fs, bool is_force = false);
  void DeleteFileSystem(uint32_t fs_id);

  Status RunOperation(Operation* operation);

  CoordinatorClientSPtr coordinator_client_;

  // for fs id
  IdGeneratorUPtr fs_id_generator_;
  // for slice id
  IdGeneratorSPtr slice_id_generator_;

  KVStorageSPtr kv_storage_;

  OperationProcessorSPtr operation_processor_;

  // notify buddy
  notify::NotifyBuddySPtr notify_buddy_;

  MDSMeta self_mds_meta_;
  MDSMetaMapSPtr mds_meta_map_;

  // protect fs_map_
  utils::RWLock lock_;
  // key: fs_id
  std::map<uint32_t, FileSystemSPtr> fs_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_H_