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

#include <signal.h>
#include <stdlib.h>

#include "absl/cleanup/cleanup.h"
#include "client/fuse/fuse_op.h"
#include "client/fuse/fuse_server.h"
#include "client/vfs_wrapper/global_log.h"

using FuseServer = dingofs::client::fuse::FuseServer;

static FuseServer* fuse_server = nullptr;

// signal handler
static void HandleSignal(int sig) {
  printf("Received signal %d, exit...\n", sig);
  if (sig == SIGHUP) {
    if (fuse_server != nullptr) {
      fuse_server->FuseSetNoUmount(true);
      fuse_server->FuseShutown();
    }
  }
}

static int InstallSignal(int sig, void (*handler)(int)) {
  struct sigaction sa;

  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;

  if (sigaction(sig, &sa, nullptr) == -1) {
    perror("dingo-fuse: cannot set signal handler");
    return -1;
  }
  return 0;
}

int main(int argc, char* argv[]) {
  struct MountOption mount_option = {nullptr};

  InstallSignal(SIGHUP, HandleSignal);

  fuse_server = new FuseServer();
  if (fuse_server == nullptr) return EXIT_FAILURE;
  auto defer_free = ::absl::MakeCleanup([&]() { delete fuse_server; });

  // init fuse
  if (fuse_server->Init(argc, argv, &mount_option) == 1) return EXIT_FAILURE;
  auto defer_uninit = ::absl::MakeCleanup([&]() { UnInitFuseClient(); });

  // init global log
  InitLog(argv[0], mount_option.conf);

  // create fuse ssssion
  if (fuse_server->FuseCreateSession() == 1) return EXIT_FAILURE;
  auto defer_destory =
      ::absl::MakeCleanup([&]() { fuse_server->FuseDestorySsesion(); });

  // mount filesystem
  if (fuse_server->FuseSessionMount() == 1) return EXIT_FAILURE;
  auto defer_unmount =
      ::absl::MakeCleanup([&]() { fuse_server->FuseSessionUnmount(); });

  // init fuse client
  if (InitFuseClient(argv[0], &mount_option) != 0) {
    LOG(ERROR) << "init fuse client fail, conf = " << mount_option.conf;
    return EXIT_FAILURE;
  }

  if (fuse_server->FuseServe() == 1) return EXIT_FAILURE;

  return EXIT_SUCCESS;
}
