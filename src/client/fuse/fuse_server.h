/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_
#define DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_

#include "client/fuse/fuse_common.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace fuse {

class FuseServer {
 public:
  explicit FuseServer();
  ~FuseServer();
  FuseServer(const FuseServer&) = delete;
  FuseServer& operator=(const FuseServer&) = delete;

  int Init(int argc, char* argv[], struct MountOption* mount_option);

  int FuseParseCmdLine();

  int FuseOptParse();

  int FuseCreateSession();

  void FuseDestorySsesion();

  int FuseSessionMount();

  void FuseSessionUnmount();

  int FuseServe();

  void FuseShutown();

  void FuseSetNoUmount(bool no_umount) { no_umount_ = no_umount; }

 private:
  void AllocateFuseInitBuf();

  void FreeFuseInitBuf();

  int FuseGetDevFd() const;

  void UdsServerFunc();

  int FuseSaveOpInitMsg();

  void FuseProcessInitMsg();

  int FuseSessionLoop();

  bool ShutdownGracefully(const char* mountpoint);

  void UdsServerStart();

  int argc_{0};
  char** argv_ = {nullptr};
  char** parsed_argv_ = {nullptr};
  const char* program_name_{nullptr};

  // libfuse
  struct fuse_args args_ = {0};
  struct fuse_cmdline_opts opts_ = {0};
  struct fuse_session* se_;
  struct fuse_loop_config config_;
  struct MountOption* mount_option_ = {nullptr};
  struct fuse_buf init_fbuf_ = {0};

  // Manager uds thread
  utils::Thread udsThread_;
  utils::Atomic<bool> isRunning_{false};

  // Manager smooth upgrade
  bool is_smooth_upgrade_{false};
  bool no_umount_{false};
  int fuse_fd_{-1};
  std::string fd_comm_path_;
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_FUSE_SERVER_H_
