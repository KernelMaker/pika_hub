//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <thread>
#include <vector>

#include "src/pika_hub_heartbeat.h"
#include "src/pika_hub_server.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "slash/include/slash_status.h"

extern PikaHubServer* g_pika_hub_server;

void* Heartbeat::ThreadMain() {
  rocksutil::Status read_status;
  pink::PinkCli* cli = nullptr;
  std::string ping = "*1\r\n$4\r\nPING\r\n";
  pink::RedisCmdArgsType pong;
  slash::Status s;
  int32_t error_times = 0;
  while (!should_stop()) {
    if (cli == nullptr) {
      cli = pink::NewRedisCli();
      cli->set_connect_timeout(1500);
      if ((cli->Connect(ip_, port_+ kPikaPortInterval)).ok()) {
        cli->set_send_timeout(3000);
        cli->set_recv_timeout(3000);
        Info(info_log_, "Heartbeat[%d] Connect to %s:%d success", server_id_,
            ip_.c_str(), port_);
        {
        rocksutil::MutexLock l(pika_mutex_);
        auto iter = pika_servers_->find(server_id_);
        if (iter != pika_servers_->end()) {
          iter->second.hb_fd = cli->fd();
        }
        }
      } else {
        Warn(info_log_, "Heartbeat[%d] Connect to %s:%d failed:%s", server_id_,
            ip_.c_str(), port_, s.ToString().c_str());
        delete cli;
        cli = nullptr;
        if ((++error_times) > kMaxRetryTimes) {
          g_pika_hub_server->DisconnectPika(server_id_);
          Error(info_log_, "Heartbeat[%d] with %s:%d disconnect", server_id_,
              ip_.c_str(), port_);
          break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
        continue;
      }
    }

    s = cli->Send(&ping);
    if (!s.ok()) {
      Warn(info_log_, "Heartbeat[%d] Send to %s:%d failed:%s", server_id_,
          ip_.c_str(), port_, s.ToString().c_str());
      if (s.IsIOError() || (++error_times) > kMaxRetryTimes) {
        delete cli;
        cli = nullptr;
        g_pika_hub_server->DisconnectPika(server_id_);
        Error(info_log_, "Heartbeat[%d] with %s:%d disconnect", server_id_,
            ip_.c_str(), port_);
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }

    s = cli->Recv(&pong);
    if (!s.ok()) {
      Warn(info_log_, "Heartbeat[%d] Recv from %s:%d failed:%s", server_id_,
          ip_.c_str(), port_, s.ToString().c_str());
      if (s.IsIOError() || (++error_times) > kMaxRetryTimes) {
        delete cli;
        cli = nullptr;
        g_pika_hub_server->DisconnectPika(server_id_);
        Error(info_log_, "Heartbeat[%d] with %s:%d disconnect", server_id_,
            ip_.c_str(), port_);
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }
    error_times = 0;
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }
  delete cli;
  return nullptr;
}
