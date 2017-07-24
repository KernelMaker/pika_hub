//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_BINLOG_WRITER_H_
#define SRC_PIKA_HUB_BINLOG_WRITER_H_

#include <string>

#include "rocksutil/log_writer.h"
#include "rocksutil/mutexlock.h"
#include "rocksutil/env.h"
#include "rocksutil/slice.h"

class BinlogManager;
class BinlogWriter {
 public:
  BinlogWriter(rocksutil::log::Writer* writer,
     uint64_t number, const std::string& log_path,
     rocksutil::Env* env,
     BinlogManager* manager)
  : writer_(writer), log_path_(log_path),
    number_(number), env_(env),
    manager_(manager), count_(0) {}

  ~BinlogWriter() {
    delete writer_;
  }

  uint64_t GetOffsetInFile();
  rocksutil::Status Append(uint8_t op, const std::string& key,
      const std::string& value, int32_t server_id,
      int32_t exec_time);

  uint64_t number() {
    return number_;
  }

  static void CacheEntityDeleter(const rocksutil::Slice& key, void* value);

  class Task {
   public:
    Task(uint8_t op, const std::string& key,
        const std::string& value, int32_t server_id,
        int32_t exec_time) :
      op_(op), key_(key), server_id_(server_id),
      exec_time_(exec_time) {
        EncodeBinlogContent(&rep_, op, key,
            value, server_id, exec_time);
    }
    uint8_t op_;
    std::string key_;
    int32_t server_id_;
    int32_t exec_time_;
    std::string rep_;
  };

  struct Executor {
    Task* task;
    bool leader;
    bool done;
    rocksutil::Status status;
    Executor* link_older;
    Executor* link_newer;
    rocksutil::port::Mutex mutex;
    rocksutil::port::CondVar cv;
    explicit Executor(Task* t) :
      task(t),
      leader(false),
      done(false),
      link_older(nullptr),
      link_newer(nullptr),
      cv(&mutex) {}
  };

  class WriteThread {
   public:
    WriteThread() :
      newest_executor_(nullptr) {}
    void JoinTaskGroup(Executor* e);
    void EnterAsTaskGroupLeader(Executor** newest_executor);
    void ExitAsTaskGroupLeader(Executor* leader, Executor* last_executor,
          const rocksutil::Status& result);


   private:
    void LinkOne(Executor* e, bool* linked_as_leader);
    std::atomic<Executor*> newest_executor_;
  };

 private:
  void RollFile();
  rocksutil::Status Append(Task* task);
  static void EncodeBinlogContent(std::string* result,
      uint8_t op, const std::string& key, const std::string& value,
      int32_t server_id, int32_t exec_time);

  rocksutil::log::Writer* writer_;
  std::string log_path_;
  uint64_t number_;
  rocksutil::Env* env_;
  BinlogManager* manager_;
  WriteThread write_thread_;
  std::atomic<int> count_;
};

extern BinlogWriter* CreateBinlogWriter(const std::string& log_path,
    uint64_t number, rocksutil::Env* env,
    BinlogManager* manager);

#endif  // SRC_PIKA_HUB_BINLOG_WRITER_H_
