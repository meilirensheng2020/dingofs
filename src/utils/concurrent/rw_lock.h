/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 18-10-11
 * Author: wudemiao
 */

#ifndef SRC_COMMON_CONCURRENT_RW_LOCK_H_
#define SRC_COMMON_CONCURRENT_RW_LOCK_H_

#include <assert.h>
#include <bthread/bthread.h>
#include <glog/logging.h>
#include <pthread.h>

#include "utils/uncopyable.h"

namespace dingofs {
namespace utils {

class RWLockBase : public Uncopyable {
 public:
  virtual ~RWLockBase() = default;

  virtual void WRLock() = 0;
  virtual int TryWRLock() = 0;
  virtual void RDLock() = 0;
  virtual int TryRDLock() = 0;
  virtual void Unlock() = 0;

 protected:
  RWLockBase() = default;
};

class PthreadRWLockBase : public RWLockBase {
 public:
  ~PthreadRWLockBase() override = default;

  void WRLock() override {
    int ret = pthread_rwlock_wrlock(&rwlock_);
    CHECK(0 == ret) << "wlock failed: " << ret << ", " << strerror(ret);
  }

  int TryWRLock() override { return pthread_rwlock_trywrlock(&rwlock_); }

  void RDLock() override {
    int ret = pthread_rwlock_rdlock(&rwlock_);
    CHECK(0 == ret) << "rlock failed: " << ret << ", " << strerror(ret);
  }

  int TryRDLock() override { return pthread_rwlock_tryrdlock(&rwlock_); }

  void Unlock() override { pthread_rwlock_unlock(&rwlock_); }

 protected:
  PthreadRWLockBase() = default;

  pthread_rwlock_t rwlock_;
  pthread_rwlockattr_t rwlockAttr_;
};

class PthreadRWLock : public PthreadRWLockBase {
 public:
  PthreadRWLock() { pthread_rwlock_init(&rwlock_, nullptr); }

  ~PthreadRWLock() override { pthread_rwlock_destroy(&rwlock_); }
};

class WritePreferedRWLock : public PthreadRWLockBase {
 public:
  WritePreferedRWLock() {
    pthread_rwlockattr_init(&rwlockAttr_);
    pthread_rwlockattr_setkind_np(&rwlockAttr_,
                                  PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

    pthread_rwlock_init(&rwlock_, &rwlockAttr_);
  }

  ~WritePreferedRWLock() override {
    pthread_rwlockattr_destroy(&rwlockAttr_);
    pthread_rwlock_destroy(&rwlock_);
  }
};

class BthreadRWLock : public RWLockBase {
 public:
  BthreadRWLock() { bthread_rwlock_init(&rwlock_, nullptr); }

  ~BthreadRWLock() override { bthread_rwlock_destroy(&rwlock_); }

  void WRLock() override {
    int ret = bthread_rwlock_wrlock(&rwlock_);
    CHECK(0 == ret) << "wlock failed: " << ret << ", " << strerror(ret);
  }

  int TryWRLock() override { return bthread_rwlock_trywrlock(&rwlock_); }

  void RDLock() override {
    int ret = bthread_rwlock_rdlock(&rwlock_);
    CHECK(0 == ret) << "rlock failed: " << ret << ", " << strerror(ret);
  }

  int TryRDLock() override { return bthread_rwlock_tryrdlock(&rwlock_); }

  void Unlock() override { bthread_rwlock_unlock(&rwlock_); }

 private:
  bthread_rwlock_t rwlock_;
};  // BthreadRWLock

class ReadLockGuard : public Uncopyable {
 public:
  explicit ReadLockGuard(RWLockBase& rwlock) : rwlock_(rwlock) {
    rwlock_.RDLock();
  }

  ~ReadLockGuard() { rwlock_.Unlock(); }

 private:
  RWLockBase& rwlock_;
};  // ReadLockGuard class

class WriteLockGuard : public Uncopyable {
 public:
  explicit WriteLockGuard(RWLockBase& rwlock) : rwlock_(rwlock) {
    rwlock_.WRLock();
  }

  ~WriteLockGuard() { rwlock_.Unlock(); }

 private:
  RWLockBase& rwlock_;
};  // WriteLockGuard class

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_COMMON_CONCURRENT_RW_LOCK_H_
