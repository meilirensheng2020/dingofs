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

#ifndef DINGOFS_SRC_CLIENT_FUSE_FUSE_CLIENT_H_
#define DINGOFS_SRC_CLIENT_FUSE_FUSE_CLIENT_H_

#include "client/fuse/fuse_common.h"
#include "client/fuse/fuse_guard.h"

namespace dingofs {
namespace client {
namespace fuse {

class FuseClient {
 public:
  explicit FuseClient(int argc, char* argv[]);
  ~FuseClient();
  FuseClient(const FuseClient&) = delete;
  FuseClient& operator=(const FuseClient&) = delete;

  int FuseParseCmdLine();

  int FuseOptParse();

  int FuseServe();

 private:
  int FuseCreateSession();

  int FuseSessionMount();

  void FuseSessionUnmount();

  int FuseSaveOpInitMsg();

  int FuseSessionLoop();

  int argc_{0};
  char** argv_ = {nullptr};
  const char* program_name_{nullptr};
  struct fuse_args args_ = {0};
  struct fuse_cmdline_opts opts_ = {0};
  struct fuse_session* se_;
  struct fuse_loop_config config_;
  char** parsed_argv_ = {nullptr};
  struct MountOption mount_option_ = {nullptr};
  FuseGuard* guard_ = {nullptr};
  struct fuse_buf init_fbuf_ = {0};
};

}  // namespace fuse
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_FUSE_FUSE_CLIENT_H_
