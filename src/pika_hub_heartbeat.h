//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_HEARTBEAT_H_
#define SRC_PIKA_HUB_HEARTBEAT_H_

#include <memory>
#include <string>

#include "src/pika_hub_common.h"
#include "pink/include/pink_thread.h"
#include "rocksutil/mutexlock.h"
#include "rocksutil/auto_roll_logger.h"

class Heartbeat : public pink::Thread {
 public:
  Heartbeat(int32_t server_id, const std::string& ip,
      const int32_t port,
      std::shared_ptr<rocksutil::Logger> info_log,
    PikaServers* pika_servers,
    rocksutil::port::Mutex* pika_mutex)
  : server_id_(server_id),
    ip_(ip), port_(port),
    info_log_(info_log),
    pika_servers_(pika_servers),
    pika_mutex_(pika_mutex) {}

  virtual ~Heartbeat() {
    set_should_stop();
    StopThread();
  }

 private:
  int32_t server_id_;
  std::string ip_;
  int32_t port_;
  std::shared_ptr<rocksutil::Logger> info_log_;
  PikaServers* pika_servers_;
  // protect pika_servers_
  rocksutil::port::Mutex* pika_mutex_;

  virtual void* ThreadMain() override;
};

#endif  // SRC_PIKA_HUB_HEARTBEAT_H_
