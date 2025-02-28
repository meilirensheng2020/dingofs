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
#include <iostream>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_format.h"
#include "client/fuse/fuse_lowlevel_ops_func.h"
#include "client/fuse/fuse_parse.h"
#include "client/fuse/fuse_passfd.h"
#include "stub/common/version.h"
#include "utils/concurrent/concurrent.h"

#define UDS_PATH "/tmp/fd_comm_sock.%d"

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

  return 0;
}

FuseServer::~FuseServer() {
  FreeFuseInitBuf();
  std::cout << "  free(opts.mountpoint)" << std::endl;
  free(opts_.mountpoint);
  // free all allocated fuse args memory
  std::cout << "  FreeParsedArgv" << std::endl;
  FreeParsedArgv(parsed_argv_, argc_);
  std::cout << "  fuse_opt_free_args" << std::endl;
  fuse_opt_free_args(&args_);
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
  LOG(INFO) << "start shutdown fuse session";
  fuse_session_exit(se_);
}

void FuseServer::UdsServerFunc() {
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
    LOG(ERROR) << "uds server create failed, error: " << std::strerror(errno);
    return;
  }
  // bind address
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, uds_path, sizeof(addr.sun_path) - 1);
  unlink(uds_path);
  if (bind(server_fd, (struct sockaddr*)&addr, addrlen) == -1) {
    LOG(ERROR) << "uds server bind failed, error: " << std::strerror(errno);
    return;
  }
  // listening
  if (listen(server_fd, 1) == -1) {
    LOG(ERROR) << "uds server listen failed, error: " << std::strerror(errno);
    return;
  }

  printf("UDS Server listening on %s\n", uds_path);
  LOG(INFO) << "uds server listening on " << uds_path;
  while (true) {
    // accept uds client
    client_fd = accept(server_fd, (struct sockaddr*)&addr, &addrlen);
    if (client_fd == -1) {
      LOG(ERROR) << "uds server accept failed, error: " << std::strerror(errno);
      continue;
    }

    LOG(INFO) << "client connected, clientid: " << client_fd;
    printf("Client connected, clientid=%d,fusefd=%d\n", client_fd,
           FuseGetDevFd());

    // process uds client request
    int fuse_fd = FuseGetDevFd();
    int ret = SendFd(client_fd, fuse_fd, init_fbuf_.mem, init_fbuf_.size);
    for (int i = 0; i < init_fbuf_.size; i++) {
      printf("%02x", ((unsigned char*)init_fbuf_.mem)[i]);
    }
    printf("\n");
    if (ret == -1) {
      LOG(INFO) << "send mount fd connected, clientid: " << client_fd;
      close(client_fd);
      continue;
    }
    LOG(INFO) << "mount fd send to client: " << client_fd << ", fd: " << fuse_fd
              << ", data size: " << init_fbuf_.size;
    printf("File descriptor sent to client, fd[%d], datasize[%zu]\n", fuse_fd,
           init_fbuf_.size);
    close(client_fd);
  }
}

void FuseServer::UdsServerStart() {
  if (isRunning_.load()) {
    LOG(INFO) << "fuse uds server already started ";
    return;
  }
  udsThread_ = utils::Thread(&FuseServer::UdsServerFunc, this);
  udsThread_.detach();
  isRunning_.store(true);
  LOG(INFO) << "fuse uds server started";
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
  std::string arg_value;

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
  LOG(INFO) << "fuse destory session";
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
      LOG(ERROR) << "smart upgrade failed, can't mount on: "
                 << opts_.mountpoint;
      return 1;
    }
    is_smart_upgrade_ = true;
    LOG(INFO) << "old dingo-fuse is already shutdown" << std::endl;
    std::string mountpoint = StrFormat("/dev/fd/%d", fuse_fd_);
    LOG(INFO) << "start mount on mountpoint: " << mountpoint;
    std::cout << "smart upgrade mountpoint: " << mountpoint << std::endl;
    if (fuse_session_mount(se_, mountpoint.c_str()) != 0) return 1;
    return 0;
  }
  // start new mount filesystem
  if (fuse_session_mount(se_, opts_.mountpoint) != 0) return 1;

  return 0;
}

int FuseServer::FuseSaveOpInitMsg() {
  // smart upgrade will not save fuse init message
  // it's recv from old dingo-fuse
  if (is_smart_upgrade_) return 0;

  int ret = 0;
  struct fuse_buf fbuf = {
      .mem = nullptr,
  };

  ret = fuse_session_receive_buf(se_, &fbuf);
  if (ret > 0) {
    LOG(INFO) << "recv fuse init message, size = " << fbuf.size;
    // save fuse init message
    CHECK(init_fbuf_.mem_size >= fbuf.size);
    init_fbuf_.size = fbuf.size;
    memcpy(init_fbuf_.mem, fbuf.mem, fbuf.size);
    fuse_session_process_buf(se_, &fbuf);
    return 0;
  }
  // here shouble call fuse_buf_free(&fbuf);
  // but function fuse_buf_free in libfuse is private, so we can not call it.
  // In special case, there may be a memory leak here,but rarely occurs and can
  // be tolerated.
  // fuse_buf_free(&fbuf);
  fuse_session_reset(se_);
  return 1;
}

void FuseServer::FuseProcessInitMsg() {
  std::cout << "init_fbuf_.size:" << init_fbuf_.size << std::endl;
  //打印16进制mem
  for (int i = 0; i < init_fbuf_.size; i++) {
    printf("%02x", ((unsigned char*)init_fbuf_.mem)[i]);
  }
  fuse_session_process_buf(se_, &init_fbuf_);
}

int FuseServer::FuseSessionLoop() {
  if (is_smart_upgrade_) {
    std::cout << "process fuse init message for smart upgrade" << std::endl;
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

  LOG(INFO) << "fuse start loop, singlethread = " << opts_.singlethread
            << ", max_idle_threads = " << opts_.max_idle_threads;

  if (FuseSaveOpInitMsg() == 1) {
    LOG(ERROR) << "save fuse init message failed";
    return 1;
  }
  /* Block until ctrl+c or fusermount -u */
  int ret = FuseSessionLoop();
  LOG(INFO) << "fuse is shutdown, ret = " << ret;
  return ret;
}

void FuseServer::FuseSessionUnmount() {
  LOG(INFO) << "fuse session umount";
  if (no_umount_) {
    LOG(INFO) << "during the smooth upgrade process, the file system will not "
                 "be unmounted";
    return;
  }
  if (!is_smart_upgrade_) {
    fuse_session_unmount(se_);
  } else {
    // smart upgrade will be umount by DingoSessionUnmount instead of
    // fuse_session_unmount because se_->mountpoint is nullptr
    DingoSessionUnmount(opts_.mountpoint, FuseGetDevFd());
  }
}

// TODO: check the fstype to determain the dingo-fuse
bool FuseServer::ShutdownGracefully(const char* mountpoint) {
  std::string file_name = StrFormat("%s/%s", mountpoint, dingofs::STATSNAME);
  int pid = GetDingoFusePid(file_name);
  if (pid == -1) {
    return false;
  }
  std::string comm_path = StrFormat(UDS_PATH, pid);

  // recv /dev/fuse fd、fuse_init data from old dingo-fuse
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
  // for (int i = 0; i < 104; i++) {
  //   printf("%02x", ((unsigned char*)init_fbuf_.mem)[i]);
  // }
  LOG(INFO) << "recv data from" << comm_path << ", mount fd = " << fuse_fd_
            << ",data size = " << init_fbuf_.size << std::endl;
  // send kill signal to old dingo-fuse
  bool is_alive = true;
  kill(pid, SIGHUP);
  for (int i = 0; i < 100; i++) {
    std::cout << "check is alive:" << i << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    is_alive = CheckProcessAlive(pid);
    LOG(INFO) << "check old dingo-fuse is alive: " << (is_alive ? "YES" : "NO")
              << std::endl;
    if (!is_alive) break;
  }
  return !is_alive;
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
