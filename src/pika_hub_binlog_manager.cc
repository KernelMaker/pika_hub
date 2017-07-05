#include "pika_hub_binlog_manager.h"
#include "pika_hub_common.h"

void AbstractFileAndUpdate(const std::string& filename,
    uint64_t* largest) {
  std::string prefix = filename.substr(0, kBinlogPrefix.size());
  if (prefix == kBinlogPrefix) {
    char* ptr;
    uint64_t num =
        strtoul(filename.data() + kBinlogPrefix.size(), &ptr, 10);
    if (num > *largest) {
      *largest = num;
    }
  }
}

BinlogWriter* BinlogManager::AddWriter() {
  return CreateBinlogWriter(log_path_, number_,
      env_, this);
}

BinlogReader* BinlogManager::AddReader(uint64_t number,
    uint64_t offset) {
  return CreateBinlogReader(log_path_, env_,
      number, offset, this);
}

void BinlogManager::UpdateWriterOffset(uint64_t number,
    uint64_t offset) {
  number_ = number;
  offset_ = offset;
}

void BinlogManager::GetWriterOffset(uint64_t* number,
    uint64_t* offset) {
  *number = number_;
  *offset = offset_;
}


BinlogManager* CreateBinlogManager(const std::string& log_path,
    rocksutil::Env* env) {
  std::vector<std::string> result;
  rocksutil::Status s = env->GetChildren(log_path, &result);

  if (!s.ok()) {
    return nullptr;
  }
  
  uint64_t largest = 0;
  for(auto& file : result) {
    AbstractFileAndUpdate(file, &largest);
  }

  largest++;

  return new BinlogManager(log_path, env, largest);
}
