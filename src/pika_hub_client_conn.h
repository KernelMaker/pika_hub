#ifndef PIKA_HUB_CLIENT_CONN_H_
#define PIKA_HUB_CLIENT_CONN_H_
#include "pink/include/redis_conn.h"

class PikaHubClientConn : public pink::RedisConn {
 public:
  PikaHubClientConn(int fd, const std::string& ip_port,
      pink::ServerThread* server_thread) :
    pink::RedisConn(fd, ip_port, server_thread) {
  };

  virtual ~PikaHubClientConn() {}

  virtual int DealMessage() override;
};

class PikaHubClientConnFactory : public pink::ConnFactory {
 public:
  explicit PikaHubClientConnFactory() {};

  virtual pink::PinkConn *NewPinkConn(int connfd,
      const std::string& ip_port,
      pink::ServerThread* server_thread,
      void* worker_private_data) const override {
    return new PikaHubClientConn(connfd, ip_port,
        server_thread);
  }
};

#endif
