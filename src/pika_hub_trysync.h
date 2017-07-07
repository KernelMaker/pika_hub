//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_TRYSYNC_H_
#define SRC_PIKA_HUB_TRYSYNC_H_

#include <string>
#include <map>
#include <memory>

#include "src/pika_hub_common.h"
#include "src/pika_hub_binlog_sender.h"
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"
#include "rocksutil/mutexlock.h"
#include "rocksutil/auto_roll_logger.h"

class PikaHubTrysync : public pink::Thread {
 public:
  PikaHubTrysync(std::shared_ptr<rocksutil::Logger> info_log,
    std::string local_ip,
    int local_port,
    std::map<std::string, PikaStatus>* pika_servers,
    rocksutil::port::Mutex* pika_mutex)
  : info_log_(info_log),
    local_ip_(local_ip),
    local_port_(local_port),
    pika_servers_(pika_servers),
    pika_mutex_(pika_mutex) {}

  virtual ~PikaHubTrysync() {
//    set_should_stop();
    set_should_stop(true);
    {
    rocksutil::MutexLock l(pika_mutex_);
    for (auto iter = pika_servers_->begin(); iter != pika_servers_->end();
        iter++) {
      delete static_cast<BinlogSender*>(iter->second.sender);
    }
    }
    StopThread();
  }

 private:
  std::shared_ptr<rocksutil::Logger> info_log_;
  std::string local_ip_;
  int local_port_;
  std::map<std::string, PikaStatus>* pika_servers_;
  // protect pika_servers_
  rocksutil::port::Mutex* pika_mutex_;

  void Trysync(const std::map<std::string, PikaStatus>::iterator& iter);
  bool Send(pink::PinkCli* cli,
        const std::map<std::string, PikaStatus>::iterator& iter);
  bool Recv(pink::PinkCli* cli,
        const std::map<std::string, PikaStatus>::iterator& iter);
  virtual void* ThreadMain() override;
};

#endif  // SRC_PIKA_HUB_TRYSYNC_H_
