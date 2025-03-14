/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: Dingofs
 * Date: 2022-02-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_METASERVER_STORAGE_STORAGE_H_
#define DINGOFS_SRC_METASERVER_STORAGE_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "metaserver/storage/config.h"
#include "metaserver/storage/iterator.h"
#include "metaserver/storage/status.h"

namespace dingofs {
namespace metaserver {
namespace storage {

using ::dingofs::metaserver::storage::Iterator;

// The interface inspired by redis, see also https://redis.io/commands
class BaseStorage {
 public:
  virtual ~BaseStorage() = default;

  // "H" is the prefix of unordered storage
  virtual Status HGet(const std::string& name, const std::string& key,
                      ValueType* value) = 0;

  virtual Status HSet(const std::string& name, const std::string& key,
                      const ValueType& value) = 0;

  virtual Status HDel(const std::string& name, const std::string& key) = 0;

  virtual std::shared_ptr<Iterator> HGetAll(const std::string& name) = 0;

  virtual size_t HSize(const std::string& name) = 0;

  virtual Status HClear(const std::string& name) = 0;

  // "S" is the prefix of ordered storage
  virtual Status SGet(const std::string& name, const std::string& key,
                      ValueType* value) = 0;

  virtual Status SSet(const std::string& name, const std::string& key,
                      const ValueType& value) = 0;

  virtual Status SDel(const std::string& name, const std::string& key) = 0;

  virtual std::shared_ptr<Iterator> SSeek(const std::string& name,
                                          const std::string& prefix) = 0;

  virtual std::shared_ptr<Iterator> SGetAll(const std::string& name) = 0;

  virtual size_t SSize(const std::string& name) = 0;

  virtual Status SClear(const std::string& name) = 0;
};

class StorageTransaction : public BaseStorage {
 public:
  virtual Status Commit() = 0;

  virtual Status Rollback() = 0;
};

class KVStorage : public BaseStorage {
 public:
  enum class STORAGE_TYPE {
    MEMORY_STORAGE,
    ROCKSDB_STORAGE,
  };

 public:
  virtual STORAGE_TYPE Type() = 0;

  virtual bool Open() = 0;

  virtual bool Close() = 0;

  virtual StorageOptions GetStorageOptions() const = 0;

  virtual std::shared_ptr<StorageTransaction> BeginTransaction() = 0;

  // Save storage's data into the destination directory, and return relative
  // filenames of current checkpoint under the directory
  virtual bool Checkpoint(const std::string& dir,
                          std::vector<std::string>* files) = 0;

  // Recover storage from a given directory
  virtual bool Recover(const std::string& dir) = 0;
};

}  // namespace storage
}  // namespace metaserver
}  // namespace dingofs

#endif  //  DINGOFS_SRC_METASERVER_STORAGE_STORAGE_H_
