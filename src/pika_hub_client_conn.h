//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PIKA_HUB_CLIENT_CONN_H_
#define SRC_PIKA_HUB_CLIENT_CONN_H_

#include <string>

#include "pink/include/redis_conn.h"

class PikaHubClientConn : public pink::RedisConn {
 public:
  PikaHubClientConn(int fd, const std::string& ip_port,
      pink::ServerThread* server_thread) :
    pink::RedisConn(fd, ip_port, server_thread) {}

  virtual ~PikaHubClientConn() {}

  virtual int DealMessage() override;
};

class PikaHubClientConnFactory : public pink::ConnFactory {
 public:
  PikaHubClientConnFactory() {}

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string& ip_port,
      pink::ServerThread* server_thread,
      void* worker_private_data) const override {
    return new PikaHubClientConn(connfd, ip_port,
        server_thread);
  }
};

#endif  // SRC_PIKA_HUB_CLIENT_CONN_H_
