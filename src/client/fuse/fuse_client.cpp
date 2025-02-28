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
#include <poll.h>
#include <sys/mount.h>

#include <iostream>

//#include "absl/cleanup/cleanup.h"
//#include "absl/strings/str_format.h"
#include "client/fuse/fuse_log.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "stub/common/version.h"

namespace dingofs {
namespace client {
namespace fuse {

FuseClient::FuseClient(int argc, char* argv[]) : argc_(argc), argv_(argv) {
  dingofs::client::fuse::InitFuseLog("/home/yansp/logs/dingofs");

  program_name_ = argv[0];
  int parsed_argc = argc_;
  parsed_argv_ = reinterpret_cast<char**>(malloc(sizeof(char*) * argc_));
  ParseOption(argc_, argv_, &parsed_argc, parsed_argv_, &mount_option_);
  // init fuse args
  args_ = FUSE_ARGS_INIT(parsed_argc, parsed_argv_);

  guard_ = new FuseGuard();
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
  //  free all allocated fuse args memory
  std::cout << "  FreeParsedArgv" << std::endl;
  FreeParsedArgv(parsed_argv_, argc_);
  std::cout << "  fuse_opt_free_args" << std::endl;
  fuse_opt_free_args(&args_);
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
      .size = 0,
      .mem = nullptr,
  };
  ret = fuse_session_receive_buf(se_, &fbuf);
  if (ret > 0) {
    // save fuse init message
    init_fbuf_.size = fbuf.size;
    init_fbuf_.mem = (void*)malloc(fbuf.size);
    memcpy(init_fbuf_.mem, fbuf.mem, fbuf.size);
    std::cout << fbuf.size << std::endl;
    fuse_session_process_buf(se_, &fbuf);
    return 0;
  }

  free(fbuf.mem);
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
  int res;
  int fd = fuse_session_fd(se_);
  const char* mountpoint = opts_.mountpoint;

  if (fd != -1) {
    struct pollfd pfd;

    pfd.fd = fd;
    pfd.events = 0;
    res = poll(&pfd, 1, 0);

    /* Need to close file descriptor, otherwise synchronous umount
       would recurse into filesystem, and deadlock.

       Caller expects fuse_kern_unmount to close the fd, so close it
       anyway. */
    close(fd);

    /* If file poll returns POLLERR on the device file descriptor,
       then the filesystem is already unmounted or the connection
       was severed via /sys/fs/fuse/connections/NNN/abort */
    if (res == 1 && (pfd.revents & POLLERR)) return;
  }

  res = umount2(mountpoint, 2);
  if (res == 0) return;
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
