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
 * @Date: 2021-08-30 19:42:18
 * @Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_METASTORE_H_
#define DINGOFS_SRC_METASERVER_METASTORE_H_

#include <gtest/gtest_prod.h>

#include <list>
#include <map>
#include <memory>
#include <string>

#include "dingofs/proto/metaserver.pb.h"
#include "dingofs/src/common/rpc_stream.h"
#include "dingofs/src/metaserver/copyset/snapshot_closure.h"
#include "dingofs/src/metaserver/metastore_fstream.h"
#include "dingofs/src/metaserver/partition.h"
#include "dingofs/src/metaserver/storage/iterator.h"
#include "dingofs/src/metaserver/superpartition/super_partition.h"

namespace dingofs {
namespace metaserver {

namespace copyset {
class CopysetNode;
}  // namespace copyset

using ::dingofs::metaserver::superpartition::SuperPartition;

// super partition
using ::dingofs::metaserver::DeleteDirQuotaRequest;
using ::dingofs::metaserver::DeleteDirQuotaResponse;
using ::dingofs::metaserver::FlushDirUsagesRequest;
using ::dingofs::metaserver::FlushDirUsagesResponse;
using ::dingofs::metaserver::FlushFsUsageRequest;
using ::dingofs::metaserver::FlushFsUsageResponse;
using ::dingofs::metaserver::GetDirQuotaRequest;
using ::dingofs::metaserver::GetDirQuotaResponse;
using ::dingofs::metaserver::GetFsQuotaRequest;
using ::dingofs::metaserver::GetFsQuotaResponse;
using ::dingofs::metaserver::LoadDirQuotasRequest;
using ::dingofs::metaserver::LoadDirQuotasResponse;
using ::dingofs::metaserver::SetDirQuotaRequest;
using ::dingofs::metaserver::SetDirQuotaResponse;
using ::dingofs::metaserver::SetFsQuotaRequest;
using ::dingofs::metaserver::SetFsQuotaResponse;

// dentry
using dingofs::metaserver::CreateDentryRequest;
using dingofs::metaserver::CreateDentryResponse;
using dingofs::metaserver::DeleteDentryRequest;
using dingofs::metaserver::DeleteDentryResponse;
using dingofs::metaserver::GetDentryRequest;
using dingofs::metaserver::GetDentryResponse;
using dingofs::metaserver::ListDentryRequest;
using dingofs::metaserver::ListDentryResponse;

// inode
using dingofs::metaserver::BatchGetInodeAttrRequest;
using dingofs::metaserver::BatchGetInodeAttrResponse;
using dingofs::metaserver::BatchGetXAttrRequest;
using dingofs::metaserver::BatchGetXAttrResponse;
using dingofs::metaserver::CreateInodeRequest;
using dingofs::metaserver::CreateInodeResponse;
using dingofs::metaserver::CreateManageInodeRequest;
using dingofs::metaserver::CreateManageInodeResponse;
using dingofs::metaserver::CreateRootInodeRequest;
using dingofs::metaserver::CreateRootInodeResponse;
using dingofs::metaserver::DeleteInodeRequest;
using dingofs::metaserver::DeleteInodeResponse;
using dingofs::metaserver::GetInodeRequest;
using dingofs::metaserver::GetInodeResponse;
using dingofs::metaserver::UpdateInodeRequest;
using dingofs::metaserver::UpdateInodeResponse;

// partition
using dingofs::metaserver::CreatePartitionRequest;
using dingofs::metaserver::CreatePartitionResponse;
using dingofs::metaserver::DeletePartitionRequest;
using dingofs::metaserver::DeletePartitionResponse;

using ::dingofs::common::StreamConnection;
using ::dingofs::common::StreamServer;
using ::dingofs::metaserver::copyset::OnSnapshotSaveDoneClosure;
using ::dingofs::metaserver::storage::Iterator;
using S3ChunkInfoMap = google::protobuf::Map<uint64_t, S3ChunkInfoList>;

using ::dingofs::metaserver::storage::StorageOptions;

class MetaStore {
 public:
  MetaStore() = default;
  virtual ~MetaStore() = default;

  virtual bool Load(const std::string& pathname) = 0;
  virtual bool Save(const std::string& dir,
                    OnSnapshotSaveDoneClosure* done) = 0;
  virtual bool Clear() = 0;
  virtual bool Destroy() = 0;

  // super partition
  virtual MetaStatusCode SetFsQuota(const SetFsQuotaRequest* request,
                                    SetFsQuotaResponse* response) = 0;

  virtual MetaStatusCode GetFsQuota(const GetFsQuotaRequest* request,
                                    GetFsQuotaResponse* response) = 0;

  virtual MetaStatusCode FlushFsUsage(const FlushFsUsageRequest* request,
                                      FlushFsUsageResponse* response) = 0;

  virtual MetaStatusCode SetDirQuota(const SetDirQuotaRequest* request,
                                     SetDirQuotaResponse* response) = 0;

  virtual MetaStatusCode GetDirQuota(const GetDirQuotaRequest* request,
                                     GetDirQuotaResponse* response) = 0;

  virtual MetaStatusCode DeleteDirQuota(const DeleteDirQuotaRequest* request,
                                        DeleteDirQuotaResponse* response) = 0;

  virtual MetaStatusCode LoadDirQuotas(const LoadDirQuotasRequest* request,
                                       LoadDirQuotasResponse* response) = 0;

  virtual MetaStatusCode FlushDirUsages(const FlushDirUsagesRequest* request,
                                        FlushDirUsagesResponse* response) = 0;

  // partition
  virtual MetaStatusCode CreatePartition(const CreatePartitionRequest* request,
                                         CreatePartitionResponse* response) = 0;

  virtual MetaStatusCode DeletePartition(const DeletePartitionRequest* request,
                                         DeletePartitionResponse* response) = 0;

  virtual bool GetPartitionInfoList(
      std::list<PartitionInfo>* partitionInfoList) = 0;

  virtual std::shared_ptr<StreamServer> GetStreamServer() = 0;

  // dentry
  virtual MetaStatusCode CreateDentry(const CreateDentryRequest* request,
                                      CreateDentryResponse* response) = 0;

  virtual MetaStatusCode GetDentry(const GetDentryRequest* request,
                                   GetDentryResponse* response) = 0;

  virtual MetaStatusCode DeleteDentry(const DeleteDentryRequest* request,
                                      DeleteDentryResponse* response) = 0;

  virtual MetaStatusCode ListDentry(const ListDentryRequest* request,
                                    ListDentryResponse* response) = 0;

  virtual MetaStatusCode PrepareRenameTx(const PrepareRenameTxRequest* request,
                                         PrepareRenameTxResponse* response) = 0;

  // inode
  virtual MetaStatusCode CreateInode(const CreateInodeRequest* request,
                                     CreateInodeResponse* response) = 0;

  virtual MetaStatusCode CreateRootInode(const CreateRootInodeRequest* request,
                                         CreateRootInodeResponse* response) = 0;

  virtual MetaStatusCode CreateManageInode(
      const CreateManageInodeRequest* request,
      CreateManageInodeResponse* response) = 0;

  virtual MetaStatusCode GetInode(const GetInodeRequest* request,
                                  GetInodeResponse* response) = 0;

  virtual MetaStatusCode BatchGetInodeAttr(
      const BatchGetInodeAttrRequest* request,
      BatchGetInodeAttrResponse* response) = 0;

  virtual MetaStatusCode BatchGetXAttr(const BatchGetXAttrRequest* request,
                                       BatchGetXAttrResponse* response) = 0;

  virtual MetaStatusCode DeleteInode(const DeleteInodeRequest* request,
                                     DeleteInodeResponse* response) = 0;

  virtual MetaStatusCode UpdateInode(const UpdateInodeRequest* request,
                                     UpdateInodeResponse* response) = 0;

  virtual MetaStatusCode GetOrModifyS3ChunkInfo(
      const GetOrModifyS3ChunkInfoRequest* request,
      GetOrModifyS3ChunkInfoResponse* response,
      std::shared_ptr<Iterator>* iterator) = 0;

  virtual MetaStatusCode SendS3ChunkInfoByStream(
      std::shared_ptr<StreamConnection> connection,
      std::shared_ptr<Iterator> iterator) = 0;

  virtual MetaStatusCode GetVolumeExtent(const GetVolumeExtentRequest* request,
                                         GetVolumeExtentResponse* response) = 0;

  virtual MetaStatusCode UpdateVolumeExtent(
      const UpdateVolumeExtentRequest* request,
      UpdateVolumeExtentResponse* response) = 0;
};

class MetaStoreImpl : public MetaStore {
 public:
  static std::unique_ptr<MetaStoreImpl> Create(
      copyset::CopysetNode* node,
      const storage::StorageOptions& storageOptions);

  bool Load(const std::string& checkpoint) override;
  bool Save(const std::string& dir, OnSnapshotSaveDoneClosure* done) override;
  bool Clear() override;
  bool Destroy() override;

  // super partition
  MetaStatusCode SetFsQuota(const SetFsQuotaRequest* request,
                            SetFsQuotaResponse* response) override;

  MetaStatusCode GetFsQuota(const GetFsQuotaRequest* request,
                            GetFsQuotaResponse* response) override;

  MetaStatusCode FlushFsUsage(const FlushFsUsageRequest* request,
                              FlushFsUsageResponse* response) override;

  MetaStatusCode SetDirQuota(const SetDirQuotaRequest* request,
                             SetDirQuotaResponse* response) override;

  MetaStatusCode GetDirQuota(const GetDirQuotaRequest* request,
                             GetDirQuotaResponse* response) override;

  MetaStatusCode DeleteDirQuota(const DeleteDirQuotaRequest* request,
                                DeleteDirQuotaResponse* response) override;

  MetaStatusCode LoadDirQuotas(const LoadDirQuotasRequest* request,
                               LoadDirQuotasResponse* response) override;

  MetaStatusCode FlushDirUsages(const FlushDirUsagesRequest* request,
                                FlushDirUsagesResponse* response) override;

  // partition
  MetaStatusCode CreatePartition(const CreatePartitionRequest* request,
                                 CreatePartitionResponse* response) override;

  MetaStatusCode DeletePartition(const DeletePartitionRequest* request,
                                 DeletePartitionResponse* response) override;

  bool GetPartitionInfoList(
      std::list<PartitionInfo>* partitionInfoList) override;

  std::shared_ptr<StreamServer> GetStreamServer() override;

  // dentry
  MetaStatusCode CreateDentry(const CreateDentryRequest* request,
                              CreateDentryResponse* response) override;

  MetaStatusCode GetDentry(const GetDentryRequest* request,
                           GetDentryResponse* response) override;

  MetaStatusCode DeleteDentry(const DeleteDentryRequest* request,
                              DeleteDentryResponse* response) override;

  MetaStatusCode ListDentry(const ListDentryRequest* request,
                            ListDentryResponse* response) override;

  MetaStatusCode PrepareRenameTx(const PrepareRenameTxRequest* request,
                                 PrepareRenameTxResponse* response) override;

  // inode
  MetaStatusCode CreateInode(const CreateInodeRequest* request,
                             CreateInodeResponse* response) override;

  MetaStatusCode CreateRootInode(const CreateRootInodeRequest* request,
                                 CreateRootInodeResponse* response) override;

  MetaStatusCode CreateManageInode(
      const CreateManageInodeRequest* request,
      CreateManageInodeResponse* response) override;

  MetaStatusCode GetInode(const GetInodeRequest* request,
                          GetInodeResponse* response) override;

  MetaStatusCode BatchGetInodeAttr(
      const BatchGetInodeAttrRequest* request,
      BatchGetInodeAttrResponse* response) override;

  MetaStatusCode BatchGetXAttr(const BatchGetXAttrRequest* request,
                               BatchGetXAttrResponse* response) override;

  MetaStatusCode DeleteInode(const DeleteInodeRequest* request,
                             DeleteInodeResponse* response) override;

  MetaStatusCode UpdateInode(const UpdateInodeRequest* request,
                             UpdateInodeResponse* response) override;

  std::shared_ptr<Partition> GetPartition(uint32_t partitionId);

  MetaStatusCode GetOrModifyS3ChunkInfo(
      const GetOrModifyS3ChunkInfoRequest* request,
      GetOrModifyS3ChunkInfoResponse* response,
      std::shared_ptr<Iterator>* iterator) override;

  MetaStatusCode SendS3ChunkInfoByStream(
      std::shared_ptr<StreamConnection> connection,
      std::shared_ptr<Iterator> iterator) override;

  MetaStatusCode GetVolumeExtent(const GetVolumeExtentRequest* request,
                                 GetVolumeExtentResponse* response) override;

  MetaStatusCode UpdateVolumeExtent(
      const UpdateVolumeExtentRequest* request,
      UpdateVolumeExtentResponse* response) override;

 private:
  FRIEND_TEST(MetastoreTest, partition);
  FRIEND_TEST(MetastoreTest, test_inode);
  FRIEND_TEST(MetastoreTest, test_dentry);
  FRIEND_TEST(MetastoreTest, persist_success);
  FRIEND_TEST(MetastoreTest, DISABLED_persist_deleting_partition_success);
  FRIEND_TEST(MetastoreTest, persist_partition_fail);
  FRIEND_TEST(MetastoreTest, persist_dentry_fail);
  FRIEND_TEST(MetastoreTest, testBatchGetInodeAttr);
  FRIEND_TEST(MetastoreTest, testBatchGetXAttr);
  FRIEND_TEST(MetastoreTest, GetOrModifyS3ChunkInfo);
  FRIEND_TEST(MetastoreTest, GetInodeWithPaddingS3Meta);
  FRIEND_TEST(MetastoreTest, TestUpdateVolumeExtent_PartitionNotFound);
  FRIEND_TEST(MetastoreTest, persist_deleting_partition_success);

  MetaStoreImpl(copyset::CopysetNode* node,
                const StorageOptions& storageOptions);

  void PrepareStreamBuffer(butil::IOBuf* buffer, uint64_t chunkIndex,
                           const std::string& value);

  void SaveBackground(const std::string& path, DumpFileClosure* child,
                      OnSnapshotSaveDoneClosure* done);

  bool InitStorage();

  // Clear data and stop background tasks
  // REQUIRES: rwLock_ is held with write permission
  bool ClearInternal();

 private:
  RWLock rwLock_;  // protect partitionMap_
  std::shared_ptr<KVStorage> kvStorage_;
  std::unique_ptr<SuperPartition> super_partition_;
  std::map<uint32_t, std::shared_ptr<Partition>> partitionMap_;
  std::list<uint32_t> partitionIds_;

  copyset::CopysetNode* copysetNode_;

  std::shared_ptr<StreamServer> streamServer_;

  storage::StorageOptions storageOptions_;
};

}  // namespace metaserver
}  // namespace dingofs
#endif  // DINGOFS_SRC_METASERVER_METASTORE_H_
