//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_BINLOG_SENDER_H_
#define SRC_PIKA_HUB_BINLOG_SENDER_H_

#include <memory>
#include <string>

#include "src/pika_hub_binlog_reader.h"
#include "pink/include/pink_thread.h"

class BinlogSender : public pink::Thread {
 public:
  BinlogSender(const std::string& ip, const int32_t port,
    std::shared_ptr<rocksutil::Logger> info_log,
    BinlogReader* reader)
  : ip_(ip), port_(port),
    info_log_(info_log),
    reader_(reader) {}

  virtual ~BinlogSender() {
//    set_should_stop();
    set_should_stop(true);
    reader_->StopRead();
    StopThread();
    delete reader_;
  }

 private:
  std::string ip_;
  int32_t port_;
  std::shared_ptr<rocksutil::Logger> info_log_;
  BinlogReader* reader_;

  virtual void* ThreadMain() override;
};

#endif  // SRC_PIKA_HUB_BINLOG_SENDER_H_
