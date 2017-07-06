//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_BINLOG_MANAGER_H_
#define SRC_PIKA_HUB_BINLOG_MANAGER_H_

#include "src/pika_hub_binlog_writer.h"
#include "src/pika_hub_binlog_reader.h"
#include <string>

class BinlogManager {
 public:
  BinlogManager(const std::string& log_path,
      rocksutil::Env* env,
      uint64_t number)
    : log_path_(log_path), env_(env),
    number_(number), offset_(0),
    cv_(&mutex_) {}

  BinlogWriter* AddWriter();
  BinlogReader* AddReader(uint64_t number, uint64_t offset);

  rocksutil::port::Mutex* mutex() {
    return &mutex_;
  }

  rocksutil::port::CondVar* cv() {
    return &cv_;
  }

  void UpdateWriterOffset(uint64_t number, uint64_t offset);
  void GetWriterOffset(uint64_t* number, uint64_t* offset);

 private:
  std::string log_path_;
  rocksutil::Env* env_;
  uint64_t number_;
  uint64_t offset_;
  rocksutil::port::Mutex mutex_;
  rocksutil::port::CondVar cv_;
};

extern BinlogManager* CreateBinlogManager(const std::string& log_path,
    rocksutil::Env* env);

#endif  // SRC_PIKA_HUB_BINLOG_MANAGER_H_
