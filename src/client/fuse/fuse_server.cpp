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
#include <sys/socket.h>

#include <chrono>

#include "absl/cleanup/cleanup.h"
#include "client/common/share_var.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "client/fuse/fuse_parse.h"
#include "client/fuse/fuse_passfd.h"
#include "stub/common/version.h"
#include "utils/concurrent/concurrent.h"

#define FD_COMMON_PATH "/tmp/fd_comm_socket.%d"

using ::dingofs::base::string::BufToHexString;
using ::dingofs::base::string::StrFormat;
using ::dingofs::client::common::ShareVar;

namespace dingofs {
namespace client {
namespace fuse {

FuseServer::FuseServer() = default;

int FuseServer::Init(int argc, char* argv[], struct MountOption* mount_option) {
  argc_ = argc;
  argv_ = argv;
  mount_option_ = mount_option;

  program_name_ = argv[0];
  int parsed_argc = argc_;
  parsed_argv_ = reinterpret_cast<char**>(malloc(sizeof(char*) * argc_));
  ParseOption(argc_, argv_, &parsed_argc, parsed_argv_, mount_option_);
  // init fuse args
  args_ = FUSE_ARGS_INIT(parsed_argc, parsed_argv_);

  AllocateFuseInitBuf();

  if (FuseParseCmdLine() == 1) return 1;

  if (FuseOptParse() == 1) return 1;

  fd_comm_path_ = StrFormat(FD_COMMON_PATH, getpid());

  return 0;
}

FuseServer::~FuseServer() {
  unlink(fd_comm_path_.c_str());
  FreeFuseInitBuf();
  free(opts_.mountpoint);
  // free all allocated fuse args memory
  FreeParsedArgv(parsed_argv_, argc_);
  fuse_opt_free_args(&args_);
  if (no_umount_ && !is_smooth_upgrade_) {
    LOG(INFO) << "transfer dingo-fuse session to others";
  }
}
// allocate memory for fuse init message
void FuseServer::AllocateFuseInitBuf() {
  // init message size = sizeof(struct fuse_in_header) + sizeof(struct
  // fuse_init_in),256 bytes is enough
  init_fbuf_.mem_size = 256;
  init_fbuf_.size = 0;
  init_fbuf_.mem = (void*)malloc(init_fbuf_.mem_size);
  memset(init_fbuf_.mem, 0, init_fbuf_.mem_size);
}

void FuseServer::FreeFuseInitBuf() {
  if (init_fbuf_.mem != nullptr) {
    free(init_fbuf_.mem);
    init_fbuf_.mem = nullptr;
    init_fbuf_.mem_size = 0;
    init_fbuf_.size = 0;
  }
}

int FuseServer::FuseGetDevFd() const { return fuse_session_fd(se_); }

void FuseServer::FuseShutown() {
  LOG(INFO) << "start shutdown dingo-fuse";
  ShareVar::GetInstance().SetValue(common::kSmoothUpgradeOld, "YES");
  fuse_session_exit(se_);
}

void FuseServer::UdsServerFunc() {
  struct sockaddr_un addr;
  int server_fd, client_fd;
  socklen_t addrlen = sizeof(addr);

  // create uds server
  server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  auto defer_close = ::absl::MakeCleanup([&]() { close(server_fd); });
  if (server_fd == -1) {
    LOG(ERROR) << "uds server create failed, path: " << fd_comm_path_
               << ", error: " << std::strerror(errno);
    return;
  }
  // bind address
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, fd_comm_path_.c_str(), sizeof(addr.sun_path) - 1);
  unlink(fd_comm_path_.c_str());
  if (bind(server_fd, (struct sockaddr*)&addr, addrlen) == -1) {
    LOG(ERROR) << "uds server bind failed,, path: " << fd_comm_path_
               << ", error: " << std::strerror(errno);
    return;
  }
  // listening
  if (listen(server_fd, 1) == -1) {
    LOG(ERROR) << "uds server listen failed, path: " << fd_comm_path_
               << ", error: " << std::strerror(errno);
    return;
  }

  printf("uds server listening on %s\n", fd_comm_path_.c_str());
  LOG(INFO) << "uds server listening on " << fd_comm_path_;
  while (true) {
    // accept uds client
    client_fd = accept(server_fd, (struct sockaddr*)&addr, &addrlen);
    if (client_fd == -1) {
      LOG(ERROR) << "uds server accept failed, error: " << std::strerror(errno);
      continue;
    }

    // process uds client request
    int fuse_fd = FuseGetDevFd();
    int ret = SendFd(client_fd, fuse_fd, init_fbuf_.mem, init_fbuf_.size);
    if (ret == -1) {
      LOG(ERROR) << "send fuse fd failed, client_id: " << client_fd;
    } else {
      LOG(INFO) << "fuse fd send to client: " << client_fd
                << ", fd: " << fuse_fd << ", data size: " << init_fbuf_.size;
    }
    close(client_fd);
  }
}

void FuseServer::UdsServerStart() {
  if (isRunning_.load()) {
    LOG(INFO) << "dingo-fuse uds server already started ";
    return;
  }
  udsThread_ = utils::Thread(&FuseServer::UdsServerFunc, this);
  udsThread_.detach();
  isRunning_.store(true);
  LOG(INFO) << "dingo-fuse uds server started";
}

int FuseServer::FuseParseCmdLine() {
  if (fuse_parse_cmdline(&args_, &opts_) != 0) return 1;
  if (opts_.show_help) {
    printf(
        "usage: %s -o conf=/etc/dingofs/client.conf -o fsname=dingofs \\\n"
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
  if (fuse_opt_parse(&args_, mount_option_, kMountOpts, nullptr) == -1) {
    return 1;
  }
  mount_option_->mount_point = opts_.mountpoint;

  if (mount_option_->conf == nullptr || mount_option_->fs_name == nullptr ||
      mount_option_->fs_type == nullptr) {
    printf(
        "one of required options is missing, conf, fsname, fstype are "
        "required.\n");

    return 1;
  }

  //  Values shown in "df -T" and friends first column "Filesystem",DindoFS +
  //  filesystem name
  FuseAddOpts(&args_, (const char*)"subtype=dingofs");
  std::string arg_value;
  arg_value.append("fsname=DingoFS");
  arg_value.append(":");
  arg_value.append(mount_option_->fs_name);
  FuseAddOpts(&args_, arg_value.c_str());

  return 0;
}

int FuseServer::FuseCreateSession() {
  // create fuse new session
  se_ = fuse_session_new(&args_, &kFuseOp, sizeof(kFuseOp), mount_option_);
  if (se_ == nullptr) return 1;

  // install fuse signal
  if (fuse_set_signal_handlers(se_) != 0) return 1;

  return 0;
}

void FuseServer::FuseDestorySsesion() {
  LOG(INFO) << "destory dingo-fuse session";

  if (se_ != nullptr) {
    fuse_remove_signal_handlers(se_);
    fuse_session_destroy(se_);
  }
}

int FuseServer::FuseSessionMount() {
  printf("Begin to mount fs %s to %s\n", mount_option_->fs_name,
         mount_option_->mount_point);

  if (CanShutdownGracefully(opts_.mountpoint)) {
    bool is_shutdown = ShutdownGracefully(opts_.mountpoint);
    if (!is_shutdown) {
      LOG(ERROR) << "smooth upgrade failed, can't mount on: "
                 << opts_.mountpoint;
      return 1;
    }
    LOG(INFO) << "old dingo-fuse is already shutdown";

    is_smooth_upgrade_ = true;
    ShareVar::GetInstance().SetValue(common::kSmoothUpgradeNew, "YES");

    // construct new mountpoint
    std::string mountpoint = StrFormat("/dev/fd/%d", fuse_fd_);
    LOG(INFO) << "start mount on mountpoint: " << mountpoint;
    if (fuse_session_mount(se_, mountpoint.c_str()) != 0) return 1;

    return 0;
  }

  if (fuse_session_mount(se_, opts_.mountpoint) != 0) return 1;

  return 0;
}

int FuseServer::FuseSaveOpInitMsg() {
  // smooth upgrade do not save fuse init message
  // it's recv from old dingo-fuse
  if (is_smooth_upgrade_) return 0;

  int ret = 0;
  struct fuse_buf fbuf = {
      .mem = nullptr,
  };

  ret = fuse_session_receive_buf(se_, &fbuf);
  if (ret > 0) {
    LOG(INFO) << "recv dingo-fuse init message, size = " << fbuf.size;
    // save fuse init message
    CHECK(init_fbuf_.mem_size >= fbuf.size);
    init_fbuf_.size = fbuf.size;
    memcpy(init_fbuf_.mem, fbuf.mem, fbuf.size);
    fuse_session_process_buf(se_, &fbuf);

    return 0;
  }
  // here shouble call fuse_buf_free(&fbuf);
  // but function fuse_buf_free in libfuse is private, so we can not call it.
  // In special case, there may be a memory leak here,but rarely occurs and
  // can be tolerated. fuse_buf_free(&fbuf);
  fuse_session_reset(se_);

  return 1;
}

void FuseServer::FuseProcessInitMsg() {
  std::string msg =
      BufToHexString((unsigned char*)init_fbuf_.mem, init_fbuf_.size);
  LOG(INFO) << "dingo-fuse init data size: " << init_fbuf_.size << ", data: 0x"
            << msg;
  fuse_session_process_buf(se_, &init_fbuf_);
}

int FuseServer::FuseSessionLoop() {
  if (is_smooth_upgrade_) {
    FuseProcessInitMsg();
  }

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

int FuseServer::FuseServe() {
  UdsServerStart();
  fuse_daemonize(opts_.foreground);

  LOG(INFO) << "dingo-fuse start loop, singlethread = " << opts_.singlethread
            << ", max_idle_threads = " << opts_.max_idle_threads;

  if (FuseSaveOpInitMsg() == 1) {
    LOG(ERROR) << "save fuse init message failed";
    return 1;
  }
  /* Block until ctrl+c or fusermount -u */
  int ret = FuseSessionLoop();
  LOG(INFO) << "dingo-fuse is shutdown, ret = " << ret;

  return ret;
}

void FuseServer::FuseSessionUnmount() {
  if (no_umount_) {
    LOG(INFO)
        << "during the smooth upgrade process, the filesystem not unmounted";
    return;
  }

  if (is_smooth_upgrade_) {
    // smooth upgrade will be umount by DingoSessionUnmount instead of
    // fuse_session_unmount because se_->mountpoint is nullptr
    LOG(INFO) << "use DingoSessionUnmount";
    DingoSessionUnmount(opts_.mountpoint, FuseGetDevFd());
  } else {
    LOG(INFO) << "use fuse_session_unmount";
    fuse_session_unmount(se_);
  }
}

// TODO: check the fstype to determain the dingo-fuse
bool FuseServer::ShutdownGracefully(const char* mountpoint) {
  // get old dingo-fuse pid
  std::string file_name = StrFormat("%s/%s", mountpoint, dingofs::STATSNAME);
  int pid = GetDingoFusePid(file_name);
  if (pid == -1) {
    return false;
  }
  LOG(INFO) << "successfully get old dingo-fuse pid: " << pid;

  std::string pid_str = std::to_string(pid);
  ShareVar::GetInstance().SetValue(common::kOldPid, pid_str);

  // recv mount fd、fuse_init data from old dingo-fuse
  std::string comm_path = StrFormat(FD_COMMON_PATH, pid);
  fuse_fd_ = GetFuseFd(comm_path.c_str(), init_fbuf_.mem, init_fbuf_.mem_size,
                       &init_fbuf_.size);
  for (int i = 0; i < 100 && fuse_fd_ <= 2; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    fuse_fd_ = GetFuseFd(comm_path.c_str(), init_fbuf_.mem, init_fbuf_.mem_size,
                         &init_fbuf_.size);
  }
  if (fuse_fd_ <= 2) {
    LOG(ERROR) << "fail to recv mount fd from" << comm_path;
    return false;
  }
  LOG(INFO) << "recv data from" << comm_path << ", mount fd = " << fuse_fd_
            << ",data size = " << init_fbuf_.size;

  // send kill signal to old dingo-fuse
  kill(pid, SIGHUP);

  // check old dingo-fuse is alive
  bool is_alive = true;
  for (int i = 0; i < 100; i++) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    is_alive = CheckProcessAlive(pid);
    LOG(INFO) << "check old dingo-fuse is alive: " << (is_alive ? "YES" : "NO");
    if (!is_alive) break;
  }

  return !is_alive;
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
