#include "pika_hub_binlog_manager.h"
#include "pika_hub_common.h"

rocksutil::Status BinlogManager::Append(const std::string& str) {
  rocksutil::MutexLock l(&mutex_);
  rocksutil::Status s = writer_->Append(str);
  if (s.ok()) {
    UpdateOffset();
  }
  return s;
}

void BinlogManager::UpdateOffset() {
  /*
   * thread safe is guaranteed by the caller (BinlogManager)
   */
  number_ = writer_->number();
  offset_ = writer_->GetOffsetInFile();
}

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
  
  BinlogWriter* writer = CreateBinlogWriter(log_path, largest, env);

  return new BinlogManager(log_path, env, writer, largest);
}
