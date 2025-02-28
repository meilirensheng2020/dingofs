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

#include "client/fuse/fuse_server.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <iostream>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "client/fuse/fuse_log.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "stub/common/version.h"

namespace dingofs {
namespace client {
namespace fuse {

using ::absl::StrFormat;
using ::absl::StrJoin;
//  using ::absl::StrSplit;

FuseServer::FuseServer(int argc, char* argv[]) : argc_(argc), argv_(argv) {
  program_name_ = argv[0];
  int parsed_argc = argc_;
  parsed_argv_ = reinterpret_cast<char**>(malloc(sizeof(char*) * argc_));
  ParseOption(argc_, argv_, &parsed_argc, parsed_argv_, &m_opts_);

  // for (int i = 0; i < parsed_argc; i++) {
  //   std::cout << "parsed_argv[" << i << "] = " << parsed_argv_[i] <<
  //   std::endl;
  // }
  // init fuse args
  args_ = FUSE_ARGS_INIT(parsed_argc, parsed_argv_);
}

int FuseServer::FuseParseCmdLine() {
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

int FuseServer::FuseOptParse() {
  std::string arg_value;

  if (fuse_opt_parse(&args_, &m_opts_, mount_opts, nullptr) == -1) {
    return 1;
  }
  m_opts_.mountPoint = opts_.mountpoint;

  if (m_opts_.conf == nullptr || m_opts_.fsName == nullptr ||
      m_opts_.fsType == nullptr) {
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
  arg_value.append(m_opts_.fsName);
  FuseAddOpts(&args_, arg_value.c_str());

  return 0;
}

int FuseServer::FuseServe() {
  printf("Begin to mount fs %s to %s\n", m_opts_.fsName, m_opts_.mountPoint);

  std::string log_msg;

  // create fuse new session
  se_ = fuse_session_new(&args_, &kFuseOp, sizeof(kFuseOp), &m_opts_);
  if (se_ == nullptr) {
    FuseLogMessage("create fuse session failed");
    return 1;
  }
  // free fuse session when exit
  auto defer_free_session = ::absl::MakeCleanup([&]() {
    if (se_ != nullptr) {
      FuseLogMessage("destory fuse session");
      fuse_session_destroy(se_);
    }
  });

  // install fuse signal
  if (fuse_set_signal_handlers(se_) != 0) {
    return 1;
  }
  // remove fuse signal when exit
  auto defer_remove_signal = ::absl::MakeCleanup([&]() {
    if (se_ != nullptr) {
      FuseLogMessage("fuse remove signal handlers");
      fuse_remove_signal_handlers(se_);
    }
  });

  // start mount filesystem
  log_msg =
      StrFormat("start mount filesystem on mountpoint: %s", opts_.mountpoint);
  FuseLogMessage(log_msg);
  if (fuse_session_mount(se_, opts_.mountpoint) != 0) {
    log_msg = StrFormat("fuse mount failed, mountpoint %s", opts_.mountpoint);
    FuseLogMessage(log_msg);
    return 1;
  }
  // fuse umount when exit
  auto defer_fuse_unmount = ::absl::MakeCleanup([&]() {
    if (se_ != nullptr) {
      log_msg = StrFormat("umount fuse mountpoint: %s", opts_.mountpoint);
      FuseLogMessage(log_msg);
      fuse_session_unmount(se_);
    }
  });

  fuse_daemonize(opts_.foreground);

  // int fuse client
  // ret = InitFuseClient(program_name_, &m_opts_);
  if (InitFuseClient(program_name_, &m_opts_) != 0) {
    LOG(ERROR) << "init fuse client fail, conf = " << m_opts_.conf;
    // log_msg = StrFormat("init fuse client fail, conf = %s", m_opts_.conf);
    // FuseLogMessage(log_msg);
  }

  LOG(INFO) << "fuse start loop, singlethread = " << opts_.singlethread
            << ", max_idle_threads = " << opts_.max_idle_threads;

  /* Block until ctrl+c or fusermount -u */
  int ret = 0;
  if (opts_.singlethread) {
    ret = fuse_session_loop(se_);
  } else {
    config_.clone_fd = opts_.clone_fd;
    config_.max_idle_threads = opts_.max_idle_threads;
    ret = fuse_session_loop_mt(se_, &config_);
  }

  FuseLogMessage("fuse server is shutdowning");
  return ret;
}
FuseServer::~FuseServer() {
  FuseLogMessage("unint fuse client");
  UnInitFuseClient();
  FuseLogMessage("free fuse mountpoint");
  free(opts_.mountpoint);
  //  free all allocated fuse args memory
  FuseLogMessage("free fuse args");
  FreeParsedArgv(parsed_argv_, argc_);
  fuse_opt_free_args(&args_);
}

// void FuseServer::FuseParseOption() {}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
