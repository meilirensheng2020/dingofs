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

#include "client/fuse/fuse_client.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/socket.h>

#include <iostream>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "client/fuse/fuse_log.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "client/fuse/fuse_passfd.h"
#include "stub/common/version.h"
#include "utils/concurrent/concurrent.h"

#define UDS_PATH "/tmp/fd_passing_socket.%d"

namespace dingofs {
namespace client {
namespace fuse {

FuseClient::FuseClient() = default;

void FuseClient::Init(int argc, char* argv[]) {
  argc_ = argc;
  argv_ = argv;

  dingofs::client::fuse::InitFuseLog("/home/yansp/logs/dingofs");

  program_name_ = argv[0];
  int parsed_argc = argc_;
  parsed_argv_ = reinterpret_cast<char**>(malloc(sizeof(char*) * argc_));
  ParseOption(argc_, argv_, &parsed_argc, parsed_argv_, &mount_option_);
  // init fuse args
  args_ = FUSE_ARGS_INIT(parsed_argc, parsed_argv_);

  guard_ = new FuseGuard();

  auto pid = GetDingoFusePid("/home/yansp/quotafs/.stats");
  std::cout << "old FuseClient pid = " << pid << std::endl;
  // UdsServerStart();
}

FuseClient::~FuseClient() {
  if (guard_ != nullptr) {
    delete guard_;
    guard_ = nullptr;
  }
  if (init_fbuf_.mem != nullptr) {
    free(init_fbuf_.mem);
  }
  std::cout << "UnInitFuseClient " << std::endl;
  UnInitFuseClient();
  std::cout << "  free(opts.mountpoint)" << std::endl;
  free(opts_.mountpoint);
  //   free all allocated fuse args memory
  std::cout << "  FreeParsedArgv" << std::endl;
  FreeParsedArgv(parsed_argv_, argc_);
  std::cout << "  fuse_opt_free_args" << std::endl;
  fuse_opt_free_args(&args_);
}

int FuseClient::FuseGetDevFd() const { return fuse_session_fd(se_); }

const char* FuseClient::FuseGetMountPoint() const { return opts_.mountpoint; }

void FuseClient::FuseShutown() {
  fuse_session_exit(se_);
  std::cout << "fuse_session_exit:" << fuse_session_exited(se_) << std::endl;
}

void FuseClient::UdsServerFunc() {
  char uds_path[1024];
  memset(uds_path, 0, 1024);
  sprintf(uds_path, UDS_PATH, getpid());
  struct sockaddr_un addr;
  int server_fd, client_fd;
  socklen_t addrlen = sizeof(addr);

  // create uds server
  server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  auto defer_close = ::absl::MakeCleanup([&]() {
    close(server_fd);
    unlink(uds_path);
  });
  if (server_fd == -1) {
    perror("socket");
    return;
  }
  // bind address
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path) - 1);
  unlink(uds_path);
  if (bind(server_fd, (struct sockaddr*)&addr, addrlen) == -1) {
    perror("bind");
    return;
  }
  // listening
  if (listen(server_fd, 1) == -1) {
    perror("listen");
    return;
  }

  printf("Server listening on %s\n", uds_path);
  while (true) {
    // accept uds client
    client_fd = accept(server_fd, (struct sockaddr*)&addr, &addrlen);
    if (client_fd == -1) {
      perror("accept");
      continue;
    }

    printf("Client connected, clientid=%d,fuserid=%d\n", client_fd,
           FuseGetDevFd());

    // process uds client request
    int ret =
        SendFd(client_fd, FuseGetDevFd(), init_fbuf_.mem, init_fbuf_.size);

    if (ret == -1) {
      perror("send_fd");
      close(client_fd);
      continue;
    }

    printf("File descriptor sent to client\n");
    close(client_fd);
  }
}

void FuseClient::UdsServerStart() {
  if (isRunning_.load()) {
    FuseLogMessage("fuse uds server already start");
    return;
  }
  udsThread_ = utils::Thread(&FuseClient::UdsServerFunc, this);
  udsThread_.detach();
  isRunning_.store(true);
  FuseLogMessage("fuse uds server start");
}

int FuseClient::FuseParseCmdLine() {
  if (fuse_parse_cmdline(&args_, &opts_) != 0) return 1;
  if (opts_.show_help) {
    printf(
        "usage: %s -o conf=/etc/dingofs/client.conf -o fsname=testfs \\\n"
        "       -o fstype=s3 "
        "[--mdsaddr=172.20.1.10:6700,172.20.1.11:6700,172.20.1.12:6700] \\\n"
        "       [OPTIONS] <mountpoint>\n",
        program_name_);
    printf("Fuse Options:\n");
    fuse_cmdline_help();
    fuse_lowlevel_help();
    ExtraOptionsHelp();
    return 1;
  } else if (opts_.show_version) {
    dingofs::stub::common::ShowVerion();

    printf("FUSE library version %s\n", fuse_pkgversion());
    fuse_lowlevel_version();
    return 1;
  }

  if (opts_.mountpoint == nullptr) {
    printf("required option is missing: mountpoint\n");
    return 1;
  }
  return 0;
}

int FuseClient::FuseOptParse() {
  std::string arg_value;

  if (fuse_opt_parse(&args_, &mount_option_, mount_opts, nullptr) == -1) {
    return 1;
  }
  mount_option_.mountPoint = opts_.mountpoint;

  if (mount_option_.conf == nullptr || mount_option_.fsName == nullptr ||
      mount_option_.fsType == nullptr) {
    printf(
        "one of required options is missing, conf, fsname, fstype are "
        "required.\n");
    return 1;
  }

  //  Values shown in "df -T" and friends first column "Filesystem",DindoFS +
  //  filesystem name
  FuseAddOpts(&args_, (const char*)"subtype=dingofs");
  arg_value.append("fsname=DingoFS");
  arg_value.append(":");
  arg_value.append(mount_option_.fsName);
  FuseAddOpts(&args_, arg_value.c_str());

  return 0;
}

int FuseClient::FuseCreateSession() {  // create fuse new session
  se_ = fuse_session_new(&args_, &kFuseOp, sizeof(kFuseOp), &mount_option_);
  if (se_ == nullptr) return 1;
  guard_->Push([&]() {
    if (se_ != nullptr) {
      std::cout << "fuse_session_destroy " << std::endl;
      FuseLogMessage("destory fuse session");
      fuse_session_destroy(se_);
    }
  });

  // install fuse signal
  if (fuse_set_signal_handlers(se_) != 0) return 1;
  guard_->Push([&]() {
    if (se_ != nullptr) {
      std::cout << "fuse_remove_signal_handlers " << std::endl;
      FuseLogMessage("fuse remove signal handlers");
      fuse_remove_signal_handlers(se_);
    }
  });
  return 0;
}

int FuseClient::FuseSessionMount() {
  printf("Begin to mount fs %s to %s\n", mount_option_.fsName,
         mount_option_.mountPoint);
  // start mount filesystem
  if (fuse_session_mount(se_, opts_.mountpoint) != 0) {
    return 1;
  }
  guard_->Push([&]() {
    if (se_ != nullptr) {
      std::cout << "fuse_session_unmount " << std::endl;
      FuseLogMessage("umount filesystem");
      FuseSessionUnmount();
    }
  });

  return 0;
}

int FuseClient::FuseSaveOpInitMsg() {
  int ret = 0;
  // process and save fuse init message
  struct fuse_buf fbuf = {
      .mem = nullptr,
  };

  ret = fuse_session_receive_buf(se_, &fbuf);
  if (ret > 0) {
    std::cout << "fuse_session_receive_buf,size :" << fbuf.size << std::endl;
    // save fuse init message
    init_fbuf_.size = fbuf.size;
    init_fbuf_.mem = (void*)malloc(fbuf.size);
    memcpy(init_fbuf_.mem, fbuf.mem, fbuf.size);
    fuse_session_process_buf(se_, &fbuf);
    return 0;
  }
  // here shouble call fuse_buf_free(&fbuf);
  // but function fuse_buf_free in libfuse is private, so we can not call it.
  // In special case, there may be a memory leak here,but rarely occurs and can
  // be tolerated.
  // here call fuse_buf_free(&fbuf);
  fuse_session_reset(se_);
  return 1;
}

int FuseClient::FuseSessionLoop() {
  int ret = 0;
  // process user request
  if (opts_.singlethread) {
    ret = fuse_session_loop(se_);
  } else {
    config_.clone_fd = opts_.clone_fd;
    config_.max_idle_threads = opts_.max_idle_threads;
    ret = fuse_session_loop_mt(se_, &config_);
  }
  return ret;
}

int FuseClient::FuseServe() {
  if (FuseCreateSession() == 1) return 1;
  if (FuseSessionMount() == 1) return 1;

  fuse_daemonize(opts_.foreground);
  // int fuse client
  if (InitFuseClient(program_name_, &mount_option_) != 0) {
    LOG(ERROR) << "init fuse client fail, conf = " << mount_option_.conf;
    return 1;
  }
  LOG(INFO) << "fuse start loop, singlethread = " << opts_.singlethread
            << ", max_idle_threads = " << opts_.max_idle_threads;

  if (FuseSaveOpInitMsg() == 1) {
    FuseLogMessage("save fuse op_init message failed");
    return 1;
  }
  /* Block until ctrl+c or fusermount -u */
  int ret = FuseSessionLoop();
  FuseLogMessage("fuse server is shutdowning");
  return ret;
}

void FuseClient::FuseSessionUnmount() {
  if (!isSmartUpgrade_) {
    fuse_session_unmount(se_);
  } else {
    // smart upgrade will be umount by dingo-fuse
    // because se_->mountpoint is nullptr
    DingoSessionUnmount(FuseGetMountPoint(), FuseGetDevFd());
  }
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
