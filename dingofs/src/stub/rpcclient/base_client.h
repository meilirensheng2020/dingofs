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
 * Created Date: Fri Jun 11 2021
 * Author: lixiaocui
 */
#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <string>
#include <vector>

#include "dingofs/proto/mds.pb.h"
#include "dingofs/proto/metaserver.pb.h"
#include "dingofs/proto/space.pb.h"
#include "dingofs/proto/topology.pb.h"
#include "dingofs/src/stub/common/common.h"

namespace dingofs {
namespace stub {
namespace rpcclient {
using dingofs::metaserver::CreateDentryRequest;
using dingofs::metaserver::CreateDentryResponse;
using dingofs::metaserver::CreateInodeRequest;
using dingofs::metaserver::CreateInodeResponse;
using dingofs::metaserver::CreateManageInodeRequest;
using dingofs::metaserver::CreateManageInodeResponse;
using dingofs::metaserver::DeleteDentryRequest;
using dingofs::metaserver::DeleteDentryResponse;
using dingofs::metaserver::DeleteInodeRequest;
using dingofs::metaserver::DeleteInodeResponse;
using dingofs::metaserver::Dentry;
using ::dingofs::metaserver::FsFileType;
using dingofs::metaserver::GetDentryRequest;
using dingofs::metaserver::GetDentryResponse;
using dingofs::metaserver::GetInodeRequest;
using dingofs::metaserver::GetInodeResponse;
using dingofs::metaserver::Inode;
using dingofs::metaserver::ListDentryRequest;
using dingofs::metaserver::ListDentryResponse;
using dingofs::metaserver::ManageInodeType;
using dingofs::metaserver::PrepareRenameTxRequest;
using dingofs::metaserver::PrepareRenameTxResponse;
using dingofs::metaserver::UpdateInodeRequest;
using dingofs::metaserver::UpdateInodeResponse;

using dingofs::common::FSType;
using dingofs::common::PartitionInfo;
using dingofs::common::PartitionStatus;
using dingofs::common::Peer;
using dingofs::common::S3Info;
using dingofs::common::Volume;

using dingofs::mds::AllocateS3ChunkRequest;
using dingofs::mds::AllocateS3ChunkResponse;
using dingofs::mds::CommitTxRequest;
using dingofs::mds::CommitTxResponse;
using dingofs::mds::FsInfo;
using dingofs::mds::FsStatus;
using dingofs::mds::GetFsInfoRequest;
using dingofs::mds::GetFsInfoResponse;
using dingofs::mds::GetLatestTxIdRequest;
using dingofs::mds::GetLatestTxIdResponse;
using dingofs::mds::MountFsRequest;
using dingofs::mds::MountFsResponse;
using dingofs::mds::RefreshSessionRequest;
using dingofs::mds::RefreshSessionResponse;
using dingofs::mds::UmountFsRequest;
using dingofs::mds::UmountFsResponse;

using ::dingofs::stub::common::CopysetID;
using ::dingofs::stub::common::LogicPoolID;

using dingofs::mds::topology::AllocOrGetMemcacheClusterRequest;
using dingofs::mds::topology::AllocOrGetMemcacheClusterResponse;
using dingofs::mds::topology::Copyset;
using dingofs::mds::topology::CreatePartitionRequest;
using dingofs::mds::topology::CreatePartitionResponse;
using dingofs::mds::topology::GetCopysetOfPartitionRequest;
using dingofs::mds::topology::GetCopysetOfPartitionResponse;
using dingofs::mds::topology::GetMetaServerInfoRequest;
using dingofs::mds::topology::GetMetaServerInfoResponse;
using dingofs::mds::topology::GetMetaServerListInCopySetsRequest;
using dingofs::mds::topology::GetMetaServerListInCopySetsResponse;
using dingofs::mds::topology::ListPartitionRequest;
using dingofs::mds::topology::ListPartitionResponse;
using dingofs::mds::topology::PartitionTxId;
using dingofs::mds::topology::TopoStatusCode;

using ::dingofs::mds::space::AcquireBlockGroupRequest;
using ::dingofs::mds::space::AcquireBlockGroupResponse;
using ::dingofs::mds::space::AllocateBlockGroupRequest;
using ::dingofs::mds::space::AllocateBlockGroupResponse;
using ::dingofs::mds::space::ReleaseBlockGroupRequest;
using ::dingofs::mds::space::ReleaseBlockGroupResponse;

using mds::Mountpoint;

struct InodeParam {
  uint64_t fsId;
  uint64_t length;
  uint32_t uid;
  uint32_t gid;
  uint32_t mode;
  FsFileType type;
  uint64_t rdev;
  std::string symlink;
  uint64_t parent;
  ManageInodeType manageType = ManageInodeType::TYPE_NOT_MANAGE;
};

inline std::ostream& operator<<(std::ostream& os, const InodeParam& p) {
  os << "fsid: " << p.fsId << ", length: " << p.length << ", uid: " << p.uid
     << ", gid: " << p.gid << ", mode: " << p.mode << ", type: " << p.type
     << ", rdev: " << p.rdev << ", symlink: " << p.symlink;

  return os;
}

class MDSBaseClient {
 public:
  virtual ~MDSBaseClient() = default;

  virtual void MountFs(const std::string& fsName, const Mountpoint& mountPt,
                       MountFsResponse* response, brpc::Controller* cntl,
                       brpc::Channel* channel);

  virtual void UmountFs(const std::string& fsName, const Mountpoint& mountPt,
                        UmountFsResponse* response, brpc::Controller* cntl,
                        brpc::Channel* channel);

  virtual void GetFsInfo(const std::string& fsName, GetFsInfoResponse* response,
                         brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetFsInfo(uint32_t fsId, GetFsInfoResponse* response,
                         brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetMetaServerInfo(uint32_t port, std::string ip,
                                 GetMetaServerInfoResponse* response,
                                 brpc::Controller* cntl,
                                 brpc::Channel* channel);
  virtual void GetMetaServerListInCopysets(
      const LogicPoolID& logicalpooid,
      const std::vector<CopysetID>& copysetidvec,
      GetMetaServerListInCopySetsResponse* response, brpc::Controller* cntl,
      brpc::Channel* channel);

  virtual void CreatePartition(uint32_t fsID, uint32_t count,
                               CreatePartitionResponse* response,
                               brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetCopysetOfPartitions(
      const std::vector<uint32_t>& partitionIDList,
      GetCopysetOfPartitionResponse* response, brpc::Controller* cntl,
      brpc::Channel* channel);

  virtual void ListPartition(uint32_t fsID, ListPartitionResponse* response,
                             brpc::Controller* cntl, brpc::Channel* channel);

  virtual void AllocS3ChunkId(uint32_t fsId, uint32_t idNum,
                              AllocateS3ChunkResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel);

  virtual void RefreshSession(const RefreshSessionRequest& request,
                              RefreshSessionResponse* response,
                              brpc::Controller* cntl, brpc::Channel* channel);

  virtual void GetLatestTxId(const GetLatestTxIdRequest& request,
                             GetLatestTxIdResponse* response,
                             brpc::Controller* cntl, brpc::Channel* channel);

  virtual void CommitTx(const CommitTxRequest& request,
                        CommitTxResponse* response, brpc::Controller* cntl,
                        brpc::Channel* channel);

  virtual void AllocateVolumeBlockGroup(uint32_t fsId, uint32_t count,
                                        const std::string& owner,
                                        AllocateBlockGroupResponse* response,
                                        brpc::Controller* cntl,
                                        brpc::Channel* channel);

  virtual void AcquireVolumeBlockGroup(uint32_t fsId, uint64_t blockGroupOffset,
                                       const std::string& owner,
                                       AcquireBlockGroupResponse* response,
                                       brpc::Controller* cntl,
                                       brpc::Channel* channel);

  virtual void ReleaseVolumeBlockGroup(
      uint32_t fsId, const std::string& owner,
      const std::vector<dingofs::mds::space::BlockGroup>& blockGroups,
      ReleaseBlockGroupResponse* response, brpc::Controller* cntl,
      brpc::Channel* channel);

  virtual void AllocOrGetMemcacheCluster(
      uint32_t fsId, AllocOrGetMemcacheClusterResponse* response,
      brpc::Controller* cntl, brpc::Channel* channel);
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_BASE_CLIENT_H_
