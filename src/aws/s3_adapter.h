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

/*************************************************************************
    > File Name: s3_adapter.h
    > Author:
    > Created Time: Mon Dec 10 14:30:12 2018
 ************************************************************************/

#ifndef SRC_AWS_S3_ADAPTER_H_
#define SRC_AWS_S3_ADAPTER_H_

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateBucketConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <condition_variable>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>

#include "bvar/reducer.h"
#include "utils/configuration.h"
#include "utils/throttle.h"

namespace dingofs {
namespace aws {

struct GetObjectAsyncContext;
struct PutObjectAsyncContext;
class S3Adapter;

struct S3AdapterOption {
  std::string ak;
  std::string sk;
  std::string s3Address;
  std::string bucketName;
  std::string region;
  int loglevel;
  std::string logPrefix;
  int scheme;
  bool verifySsl;
  int maxConnections;
  int connectTimeout;
  int requestTimeout;
  int asyncThreadNum;
  uint64_t maxAsyncRequestInflightBytes;
  uint64_t iopsTotalLimit;
  uint64_t iopsReadLimit;
  uint64_t iopsWriteLimit;
  uint64_t bpsTotalMB;
  uint64_t bpsReadMB;
  uint64_t bpsWriteMB;
  bool useVirtualAddressing;
  bool enableTelemetry;
};

struct S3InfoOption {
  // should get from mds
  std::string ak;
  std::string sk;
  std::string s3Address;
  std::string bucketName;
  uint64_t blockSize;
  uint64_t chunkSize;
  uint32_t objectPrefix;
};

void InitS3AdaptorOptionExceptS3InfoOption(utils::Configuration* conf,
                                           S3AdapterOption* s3_opt);

void InitS3AdaptorOption(utils::Configuration* conf, S3AdapterOption* s3_opt);

using GetObjectAsyncCallBack = std::function<void(
    const S3Adapter*, const std::shared_ptr<GetObjectAsyncContext>&)>;

struct GetObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::string key;
  char* buf;
  off_t offset;
  size_t len;
  GetObjectAsyncCallBack cb;
  int retCode;
  uint32_t retry;
  size_t actualLen;
};

using PutObjectAsyncCallBack =
    std::function<void(const std::shared_ptr<PutObjectAsyncContext>&)>;

struct PutObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::string key;
  const char* buffer;
  size_t bufferSize;
  PutObjectAsyncCallBack cb;
  uint64_t startTime;
  int retCode;
};

class S3Adapter {
 public:
  S3Adapter()
      : clientCfg_(nullptr),
        s3Client_(nullptr),
        throttle_(nullptr),
        s3_object_put_async_num_("s3_object_put_async_num"),
        s3_object_put_sync_num_("s3_object_put_sync_num"),
        s3_object_get_async_num_("s3_object_get_async_num"),
        s3_object_get_sync_num_("s3_object_get_sync_num") {}

  virtual ~S3Adapter() { Deinit(); }

  /**
   * 初始化S3Adapter
   */
  virtual void Init(const std::string& path);

  /**
   * 初始化S3Adapter
   * 但不包括 S3InfoOption
   */
  virtual void InitExceptFsS3Option(const std::string& path);

  /**
   * 初始化S3Adapter
   */
  virtual void Init(const S3AdapterOption& option);

  /**
   * @brief
   *
   * @details
   */
  virtual void SetS3Option(const S3InfoOption& fs_s3_opt);

  /**
   * 释放S3Adapter资源
   */
  virtual void Deinit();

  /**
   *  call aws sdk shutdown api
   */
  static void Shutdown();

  /**
   * reinit s3client with new AWSCredentials
   */
  virtual void Reinit(const S3AdapterOption& option);

  /**
   * get s3 ak
   */
  virtual std::string GetS3Ak();

  /**
   * get s3 sk
   */
  virtual std::string GetS3Sk();

  /**
   * get s3 endpoint
   */
  virtual std::string GetS3Endpoint();

  /**
   * 创建存储快照数据的桶（桶名称由配置文件指定，需要全局唯一）
   * @return: 0 创建成功/ -1 创建失败
   */
  virtual int CreateBucket();

  /**
   * 删除桶
   * @return 0 删除成功/-1 删除失败
   */
  virtual int DeleteBucket();

  /**
   * 判断快照数据的桶是否存在
   * @return true 桶存在/ false 桶不存在
   */
  virtual bool BucketExist();

  /**
   * 上传数据到对象存储
   * @param 对象名
   * @param 数据内容
   * @param 数据内容大小
   * @return:0 上传成功/ -1 上传失败
   */
  virtual int PutObject(const Aws::String& key, const char* buffer,
                        size_t buffer_size);

  /**
   * 上传数据到对象存储
   * @param 对象名
   * @param 数据内容
   * @return:0 上传成功/ -1 上传失败
   */
  virtual int PutObject(const Aws::String& key, const std::string& data);

  virtual void PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context);

  /**
   * Get object from s3,
   * note：this function is only used for control plane to get small data,
   * it only has qps limit and dosn't have bandwidth limit.
   * For the data plane, please use the following two function.
   *
   * @param object name
   * @param pointer which contain the data
   * @return 0 success / -1 fail
   */
  virtual int GetObject(const Aws::String& key, std::string* data);

  /**
   * 从对象存储读取数据
   * @param 对象名
   * @param[out] 返回读取的数据
   * @param 读取的偏移
   * @param 读取的长度
   */
  virtual int GetObject(const std::string& key, char* buf, off_t offset,
                        size_t len);  // NOLINT

  /**
   * @brief 异步从对象存储读取数据
   *
   * @param context 异步上下文
   */
  virtual void GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context);

  /**
   * 删除对象
   * @param 对象名
   * @return: 0 删除成功/ -
   */
  virtual int DeleteObject(const Aws::String& key);

  virtual int DeleteObjects(const std::list<Aws::String>& key_list);
  /**
   * 判断对象是否存在
   * @param 对象名
   * @return: true 对象存在/ false 对象不存在
   */
  virtual bool ObjectExist(const Aws::String& key);

  /**
   * 初始化对象的分片上传任务
   * @param 对象名
   * @return 任务名
   */
  virtual Aws::String MultiUploadInit(const Aws::String& key);

  /**
   * 增加一个分片到分片上传任务中
   * @param 对象名
   * @param 任务名
   * @param 第几个分片（从1开始）
   * @param 分片大小
   * @param 分片的数据内容
   * @return: 分片任务管理对象
   */
  virtual Aws::S3::Model::CompletedPart UploadOnePart(
      const Aws::String& key, const Aws::String& upload_id, int part_num,
      int part_size, const char* buf);

  /**
   * 完成分片上传任务
   * @param 对象名
   * @param 分片上传任务id
   * @管理分片上传任务的vector
   * @return 0 任务完成/ -1 任务失败
   */
  virtual int CompleteMultiUpload(
      const Aws::String& key, const Aws::String& upload_id,
      const Aws::Vector<Aws::S3::Model::CompletedPart>& cp_v);

  /**
   * 终止一个对象的分片上传任务
   * @param 对象名
   * @param 任务id
   * @return 0 终止成功/ -1 终止失败
   */
  virtual int AbortMultiUpload(const Aws::String& key,
                               const Aws::String& upload_id);

  void SetBucketName(const Aws::String& name) { bucketName_ = name; }

  Aws::String GetBucketName() { return bucketName_; }

 private:
  class AsyncRequestInflightBytesThrottle {
   public:
    explicit AsyncRequestInflightBytesThrottle(uint64_t max_inflight_bytes)
        : maxInflightBytes_(max_inflight_bytes), inflightBytes_(0) {}

    void OnStart(uint64_t len);
    void OnComplete(uint64_t len);

   private:
    const uint64_t maxInflightBytes_;
    uint64_t inflightBytes_;

    std::mutex mtx_;
    std::condition_variable cond_;
  };

  // S3服务器地址
  Aws::String s3Address_;
  // 用于用户认证的AK/SK，需要从对象存储的用户管理中申请
  Aws::String s3Ak_;
  Aws::String s3Sk_;
  // 对象的桶名
  Aws::String bucketName_;
  // aws sdk的配置
  Aws::Client::ClientConfiguration* clientCfg_;
  Aws::S3::S3Client* s3Client_;
  utils::Configuration conf_;

  utils::Throttle* throttle_;

  std::unique_ptr<AsyncRequestInflightBytesThrottle> inflightBytesThrottle_;

  bvar::Adder<uint64_t> s3_object_put_async_num_;
  bvar::Adder<uint64_t> s3_object_put_sync_num_;
  bvar::Adder<uint64_t> s3_object_get_async_num_;
  bvar::Adder<uint64_t> s3_object_get_sync_num_;
};

class FakeS3Adapter final : public S3Adapter {
 public:
  FakeS3Adapter() = default;

  ~FakeS3Adapter() override = default;

  bool BucketExist() override { return true; }

  int PutObject(const Aws::String& key, const char* buffer,
                const size_t buffer_size) override {
    (void)key;
    (void)buffer;
    (void)buffer_size;
    return 0;
  }

  int PutObject(const Aws::String& key, const std::string& data) override {
    (void)key;
    (void)data;
    return 0;
  }

  void PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context) override {
    context->retCode = 0;
    context->cb(context);
  }

  int GetObject(const Aws::String& key, std::string* data) override {
    (void)key;
    (void)data;
    // just return 4M data
    data->resize(4 * 1024 * 1024, '1');
    return 0;
  }

  int GetObject(const std::string& key, char* buf, off_t offset,
                size_t len) override {
    (void)key;
    (void)offset;
    // juset return len data
    memset(buf, '1', len);
    return 0;
  }

  void GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context) override {
    memset(context->buf, '1', context->len);
    context->retCode = 0;
    context->cb(this, context);
  }

  int DeleteObject(const Aws::String& key) override {
    (void)key;
    return 0;
  }

  int DeleteObjects(const std::list<Aws::String>& key_list) override {
    (void)key_list;
    return 0;
  }

  bool ObjectExist(const Aws::String& key) override {
    (void)key;
    return true;
  }
};

}  // namespace aws
}  // namespace dingofs
#endif  // SRC_AWS_S3_ADAPTER_H_
