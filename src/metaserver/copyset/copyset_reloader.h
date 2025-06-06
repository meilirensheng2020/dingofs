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
 * Date: Sun 22 Aug 2021 10:40:42 AM CST
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_COPYSET_RELOADER_H_
#define DINGOFS_SRC_METASERVER_COPYSET_COPYSET_RELOADER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "fs/local_filesystem.h"
#include "metaserver/common/types.h"
#include "metaserver/copyset/copyset_node.h"
#include "utils/concurrent/task_thread_pool.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class CopysetNodeManager;

// Reload all existing copysets
class CopysetReloader {
 public:
  explicit CopysetReloader(CopysetNodeManager* copysetNodeManager);

  ~CopysetReloader() = default;

  bool Init(const CopysetNodeOptions& options);

  /**
   * @brief Reload all existing copysets
   */
  bool ReloadCopysets();

 private:
  bool ReloadOneCopyset(const std::string& copyset);

  void LoadCopyset(PoolId poolId, CopysetId copysetId);

  bool CheckCopysetUntilLoadFinished(CopysetNode* node);

  bool WaitLoadFinish();

 private:
  CopysetNodeManager* nodeManager_;
  CopysetNodeOptions options_;

  std::unique_ptr<dingofs::utils::TaskThreadPool<>> taskPool_;
  std::atomic<bool> running_;
  std::atomic<uint32_t> load_copyset_errors_{0};
};

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_COPYSET_RELOADER_H_
