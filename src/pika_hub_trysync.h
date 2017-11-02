//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_TRYSYNC_H_
#define SRC_PIKA_HUB_TRYSYNC_H_

#include <string>
#include <memory>

#include "src/pika_hub_common.h"
#include "src/pika_hub_binlog_sender.h"
#include "src/pika_hub_heartbeat.h"
#include "src/pika_hub_binlog_manager.h"
#include "pink/include/pink_cli.h"
#include "pink/include/pink_thread.h"
#include "rocksutil/mutexlock.h"
#include "rocksutil/auto_roll_logger.h"

class PikaHubTrysync : public pink::Thread {
 public:
  PikaHubTrysync(std::shared_ptr<rocksutil::Logger> info_log,
    std::string local_ip,
    int local_port,
    PikaServers* pika_servers,
    rocksutil::port::Mutex* pika_mutex,
    RecoverOffsetMap* recover_offset,
    BinlogManager* manager)
  : info_log_(info_log),
    local_ip_(local_ip),
    local_port_(local_port),
    pika_servers_(pika_servers),
    pika_mutex_(pika_mutex),
    recover_offset_(recover_offset),
    manager_(manager) {}

  virtual ~PikaHubTrysync() {
    set_should_stop();
    {
     /*
      * I commented the lock, because it may cause deadlock when BinlogSender
      * is using pika_mutex_. It looks like that there is no need to hold
      * the lock here. I will solve it later...
      */
//    rocksutil::MutexLock l(pika_mutex_);
    for (auto iter = pika_servers_->begin(); iter != pika_servers_->end();
        iter++) {
      delete static_cast<BinlogSender*>(iter->second.sender);
      iter->second.sender = nullptr;
      iter->second.send_fd = -1;
      delete static_cast<Heartbeat*>(iter->second.heartbeat);
      iter->second.heartbeat = nullptr;
      iter->second.hb_fd = -1;
    }
    }
    StopThread();
  }

 private:
  std::shared_ptr<rocksutil::Logger> info_log_;
  std::string local_ip_;
  int local_port_;
  PikaServers* pika_servers_;
  // protect pika_servers_
  rocksutil::port::Mutex* pika_mutex_;
  RecoverOffsetMap* recover_offset_;
  BinlogManager* manager_;

  void Trysync(const PikaServers::iterator& iter);
  bool Send(pink::PinkCli* cli,
        const PikaServers::iterator& iter);
  bool Recv(pink::PinkCli* cli,
        const PikaServers::iterator& iter);
  bool Auth(pink::PinkCli* cli,
        const PikaServers::iterator& iter);
  virtual void* ThreadMain() override;
};

#endif  // SRC_PIKA_HUB_TRYSYNC_H_
