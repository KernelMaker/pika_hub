//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_INNER_CLIENT_CONN_H_
#define SRC_PIKA_HUB_INNER_CLIENT_CONN_H_

#include <string>

#include "pink/include/redis_conn.h"
#include "src/pika_hub_command.h"

class PikaHubInnerClientConn : public pink::RedisConn {
 public:
  PikaHubInnerClientConn(int fd, const std::string& ip_port,
      pink::ServerThread* server_thread, void* worker_specific_data) :
    pink::RedisConn(fd, ip_port, server_thread) {}

  virtual ~PikaHubInnerClientConn() {}

  virtual int DealMessage() override;
};

class PikaHubInnerClientConnFactory : public pink::ConnFactory {
 public:
  PikaHubInnerClientConnFactory() {}

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string& ip_port,
      pink::ServerThread* server_thread,
      void* worker_private_data) const override {
    return new PikaHubInnerClientConn(connfd, ip_port,
        server_thread, worker_private_data);
  }
};

#endif  // SRC_PIKA_HUB_INNER_CLIENT_CONN_H_
