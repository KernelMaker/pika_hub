#include "pika_hub_binlog_sender.h"

void* BinlogSender::ThreadMain() {
  std::string scratch;
  rocksutil::Slice record;
  rocksutil::Status s;
  while(!should_stop()) {
    s = reader_->ReadRecord(&record, &scratch);
    if (s.ok()) {
      Info(info_log_, std::string(record.data(), record.size()).c_str());
    } else {
      Error(info_log_, s.ToString().c_str());
    }
  }
  return nullptr;
}
