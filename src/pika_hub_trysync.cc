//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <thread>

#include "src/pika_hub_trysync.h"
#include "src/pika_hub_heartbeat.h"
#include "pink/include/redis_cli.h"
#include "slash/include/slash_string.h"
#include "slash/include/slash_status.h"

bool PikaHubTrysync::Send(pink::PinkCli* cli,
    const PikaServers::iterator& iter) {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  uint64_t number =
    iter->second.rcv_number >= kMaxRecvRollbackNums ?
      iter->second.rcv_number - kMaxRecvRollbackNums : 0;

  argv.clear();
  std::string tbuf_str;
  argv.push_back("internaltrysync");
  argv.push_back(local_ip_);
  argv.push_back(std::to_string(local_port_));
  argv.push_back(std::to_string(number));
  argv.push_back(std::to_string(0));

  pink::SerializeRedisCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);

  slash::Status s = cli->Send(&wbuf_str);
  if (!s.ok()) {
    Error(info_log_, "Trysync master %d,%s:%d(%llu %llu), Send, error: %s",
      iter->first, iter->second.ip.c_str(), iter->second.port,
      number, 0,
      s.ToString().c_str());
    return false;
  }
  return true;
}

bool PikaHubTrysync::Recv(pink::PinkCli* cli,
    const PikaServers::iterator& iter) {
  slash::Status s;
  std::string reply;
  uint64_t number =
    iter->second.rcv_number >= kMaxRecvRollbackNums ?
      iter->second.rcv_number - kMaxRecvRollbackNums : 0;

  pink::RedisCmdArgsType argv;
  s = cli->Recv(&argv);
  if (!s.ok()) {
    Error(info_log_, "Trysync master %d,%s:%d(%llu %llu), Recv, error: %s",
      iter->first, iter->second.ip.c_str(), iter->second.port,
      number, 0,
      strerror(errno));
    return false;
  }

  reply = slash::StringToLower(argv[0]);

  if (reply != "ok") {
    Error(info_log_,
      "Trysync master %d,%s:%d(%llu %llu), Recv, logic error: %s",
      iter->first, iter->second.ip.c_str(), iter->second.port,
      number, 0,
      reply.c_str());
    iter->second.sync_status = kErrorHappened;
    return false;
  }
  iter->second.sync_status = kConnected;
  if (iter->second.sender == nullptr) {
    BinlogReader* reader = manager_->AddReader(iter->second.send_number,
        0);
    if (reader) {
      iter->second.sender = new BinlogSender(iter->first,
          iter->second.ip, iter->second.port, info_log_, reader,
          pika_servers_, pika_mutex_, manager_);
      static_cast<BinlogSender*>(iter->second.sender)->StartThread();
      Info(info_log_, "Start BinlogSender[%d] success for %s:%d(%llu %llu)",
          iter->first, iter->second.ip.c_str(), iter->second.port,
          iter->second.send_number, 0);
    } else {
      Error(info_log_, "Start BinlogSender[%d] Failed for %s:%d(%llu %llu)",
          iter->first, iter->second.ip.c_str(), iter->second.port,
          iter->second.send_number, 0);
    }
  }
  if (iter->second.heartbeat == nullptr) {
    iter->second.heartbeat = new Heartbeat(iter->first,
        iter->second.ip, iter->second.port, info_log_,
        pika_servers_, pika_mutex_);
    static_cast<Heartbeat*>(iter->second.heartbeat)->StartThread();
    Info(info_log_, "Start HeartBeat[%d] success for %s:%d",
        iter->first, iter->second.ip.c_str(), iter->second.port);
  }
  return true;
}

void PikaHubTrysync::Trysync(const PikaServers::
    iterator& iter) {
  pink::PinkCli* cli = pink::NewRedisCli();
  cli->set_connect_timeout(1500);
  std::string master_ip;
  uint64_t number =
    iter->second.rcv_number >= kMaxRecvRollbackNums ?
      iter->second.rcv_number - kMaxRecvRollbackNums : 0;
  if ((cli->Connect(iter->second.ip, iter->second.port)).ok()) {
    cli->set_send_timeout(3000);
    cli->set_recv_timeout(3000);
    if (Send(cli, iter) && Recv(cli, iter)) {
      Info(info_log_, "Trysync master %d,%s:%d(%llu %llu) success",
          iter->first,
          iter->second.ip.c_str(), iter->second.port,
          number, 0);
    }
  } else {
    Error(info_log_, "Trysync master %d,%s:%d(%llu %llu) connect failed",
          iter->first,
          iter->second.ip.c_str(), iter->second.port,
          number, 0);
  }
  delete cli;
}

void* PikaHubTrysync::ThreadMain() {
  while (!should_stop()) {
    {
    rocksutil::MutexLock l(pika_mutex_);
    for (auto it = pika_servers_->begin(); it != pika_servers_->end(); it++) {
      if (it->second.sync_status == kShouldDelete) {
        delete static_cast<BinlogSender*>(it->second.sender);
        it = pika_servers_->erase(it);
      }
      if (it->second.sync_status == kShouldConnect) {
        Trysync(it);
      }
    }
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  return nullptr;
}
