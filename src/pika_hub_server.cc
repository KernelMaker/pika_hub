#include "pika_hub_server.h"

int PikaHubServerConn::DealMessage() {
  strcpy(wbuf_, "+OK\r\n");
  wbuf_len_ = 5;
  set_is_reply(true);
  return 0;
}

PikaHubServer::PikaHubServer(int sdk_port, const floyd::Options& options)
  : last_query_num_(0),
    query_num_(0),
    last_time_us_(slash::NowMicros()) {
  floyd::Floyd::Open(options, &floyd_);
  conn_factory_ = new PikaHubServerConnFactory(); 
  server_handler_ = new PikaHubServerHandler();
  server_thread_ = pink::NewHolyThread(sdk_port, conn_factory_, 0, server_handler_);
}

PikaHubServer::~PikaHubServer() {
  server_thread_->StopThread();
  delete server_thread_;
  delete conn_factory_;
  delete server_handler_;
  delete floyd_;
}

slash::Status PikaHubServer::Start() {
  slash::Status result = floyd_->Start();
  if (!result.ok()) {
    std::cout << "Floyd start failed" << std::endl;
    return result;
  }

  int ret = server_thread_->StartThread();
  if (ret != 0) {
    return slash::Status::Corruption("Start server error");
  }
  std::cout << "Started" << std::endl;
  server_mutex_.Lock();
  server_mutex_.Lock();
  server_mutex_.Unlock();
  return slash::Status::OK();
}
