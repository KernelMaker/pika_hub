#include "pika_hub_binlog_writer.h"
#include "pika_hub_common.h"
#include "rocksutil/file_reader_writer.h"

rocksutil::Status BinlogWriter::Append(const std::string& str) {
  rocksutil::MutexLock l(&mutex_);
  if (writer_->file()->GetFileSize() >= kMaxBinlogFileSize) {
    RollFile();
  }
  return writer_->AddRecord(str);
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

rocksutil::log::Writer* CreateWriter(rocksutil::Env* env,
    const std::string log_path, uint64_t num) {

  rocksutil::EnvOptions env_options;
  env_options.use_mmap_reads = false;
  env_options.use_mmap_writes = false;
  std::unique_ptr<rocksutil::WritableFile> writable_file;
  std::string filename = log_path + "/" + kBinlogPrefix + std::to_string(num);
  rocksutil::Status s = NewWritableFile(env, filename,
                &writable_file, env_options);
  if (!s.ok()) {
    return nullptr;
  }

  std::unique_ptr<rocksutil::WritableFileWriter> writable_file_writer(
       new rocksutil::WritableFileWriter(std::move(writable_file), env_options));

  return new rocksutil::log::Writer(std::move(writable_file_writer));
}

void BinlogWriter::RollFile() {
  rocksutil::log::Writer* new_writer = CreateWriter(env_,
      log_path_, number_ + 1);
  if (new_writer != nullptr) {
    delete writer_;
    writer_ = new_writer;
    number_++;
  }
}

BinlogWriter* CreateBinlogWriter(const std::string& log_path,
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
  
  rocksutil::log::Writer* writer = CreateWriter(env,
      log_path, largest);
  return (writer == nullptr ?
      nullptr : new BinlogWriter(writer, largest, log_path, env));
}
