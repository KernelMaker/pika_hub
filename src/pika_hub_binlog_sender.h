#ifndef PIKA_HUB_BINLOG_SENDER_
#define PIKA_HUB_BINLOG_SENDER_
#include "rocksutil/auto_roll_logger.h"
#include "pika_hub_binlog_reader.h"
#include "pink/include/pink_thread.h"

class BinlogSender : public pink::Thread {
 public:
  BinlogSender(const std::string& ip, const int32_t port,
    std::shared_ptr<rocksutil::Logger> info_log,
    BinlogReader* reader)
  : ip_(ip), port_(port),
    info_log_(info_log),
    reader_(reader) {
  };
  virtual ~BinlogSender() {
    set_should_stop(true);
    reader_->StopRead();
    StopThread();
    delete reader_;
  };

 private:
  std::string ip_;
  int32_t port_;
  std::shared_ptr<rocksutil::Logger> info_log_;
  BinlogReader* reader_;

  virtual void* ThreadMain();
};

#endif
