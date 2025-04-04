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
 * Created Date: 2021-10-19
 * Author: chengyi01
 */
#include "tools/query/dingofs_metaserver_query.h"

DECLARE_string(metaserverId);
DECLARE_string(metaserverAddr);
DECLARE_string(mdsAddr);

namespace dingofs {
namespace tools {
namespace query {

void MetaserverQueryTool::PrintHelp() {
  DingofsToolRpc::PrintHelp();
  std::cout << " -metaserverId=" << FLAGS_metaserverId
            << "(matter)|-metaserverAddr=" << FLAGS_metaserverAddr
            << " [-mdsAddr=" << FLAGS_mdsAddr << "]";
  std::cout << std::endl;
}

int MetaserverQueryTool::Init() {
  if (DingofsToolRpc::Init() != 0) {
    return -1;
  }

  dingofs::utils::SplitString(FLAGS_mdsAddr, ",", &hostsAddr_);

  std::vector<std::string> metaserver_id_vec;
  std::vector<std::string> metaserver_addr_vec;
  google::CommandLineFlagInfo info;
  if (CheckMetaserverIdDefault(&info) && !CheckMetaserverAddrDefault(&info)) {
    // only use mateserverAddr in this case
    dingofs::utils::SplitString(FLAGS_metaserverAddr, ",",
                                &metaserver_addr_vec);
  } else {
    dingofs::utils::SplitString(FLAGS_metaserverId, ",", &metaserver_id_vec);
  }

  for (auto const& i : metaserver_addr_vec) {
    std::string ip;
    uint32_t port;
    if (mds::topology::SplitAddrToIpPort(i, &ip, &port)) {
      pb::mds::topology::GetMetaServerInfoRequest request;
      request.set_hostip(ip);
      request.set_port(port);
      requestQueue_.push(request);
    } else {
      std::cerr << "metaserverAddr:" << i << " is invalid, please check it."
                << std::endl;
    }
  }

  for (auto const& i : metaserver_id_vec) {
    pb::mds::topology::GetMetaServerInfoRequest request;
    request.set_metaserverid(std::stoul(i));
    requestQueue_.push(request);
  }

  service_stub_func_ =
      std::bind(&pb::mds::topology::TopologyService_Stub::GetMetaServer,
                service_stub_.get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3, nullptr);
  return 0;
}

void MetaserverQueryTool::AddUpdateFlags() {
  AddUpdateFlagsFunc(dingofs::tools::SetMdsAddr);
}

bool MetaserverQueryTool::AfterSendRequestToHost(const std::string& host) {
  if (controller_->Failed()) {
    errorOutput_ << "send query metaserver \n"
                 << requestQueue_.front().DebugString() << "\nto mds: " << host
                 << " failed, errorcode= " << controller_->ErrorCode()
                 << ", error text " << controller_->ErrorText() << "\n";
    return false;
  } else if (show_) {
    std::cout << response_->DebugString() << std::endl;
  }

  return true;
}

bool MetaserverQueryTool::CheckRequiredFlagDefault() {
  google::CommandLineFlagInfo info;
  if (CheckMetaserverIdDefault(&info) && CheckMetaserverAddrDefault(&info)) {
    std::cerr << "no -metaserverId=*,*|-metaserverAddr=*:*,*:*, please use "
                 "-example!"
              << std::endl;
    return true;
  }
  return false;
}

}  // namespace query
}  // namespace tools
}  // namespace dingofs
