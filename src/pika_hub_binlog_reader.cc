#include "pika_hub_binlog_reader.h"
#include "pika_hub_common.h"
#include "rocksutil/file_reader_writer.h"
#include "rocksutil/log_reader.h"

#include <iostream>

rocksutil::Status BinlogReader::ReadRecord(rocksutil::Slice* slice,
    std::string* scratch) {
  bool ret = true;
  while (true) {
    ret = reader_->ReadRecord(slice, scratch);
    if (ret) {
      offset_ += slice->size();
      return rocksutil::Status::OK();
    } else {
      std::cout << status_.ToString() << std::endl;
      if (IsEOF()) {
        if (TryToRollFile()) {
          continue;
        } else {
          return rocksutil::Status::NotFound("No More Record");
        }
      } else {
        return status_;
      }
    }
  }
}

rocksutil::log::Reader* CreateReader(rocksutil::Env* env,
    const std::string log_path, uint64_t num,
    uint64_t offset, rocksutil::log::Reader::LogReporter* reporter) {

  rocksutil::EnvOptions env_options;
  env_options.use_mmap_reads = false;
  env_options.use_mmap_writes = false;

  std::unique_ptr<rocksutil::SequentialFile> sequential_file;
  std::string filename = log_path + "/" + kBinlogPrefix + std::to_string(num);
  rocksutil::Status s = rocksutil::NewSequentialFile(env, filename,
                            &sequential_file, env_options);
  if (!s.ok()) {
    return nullptr;
  }
  std::unique_ptr<rocksutil::SequentialFileReader> sequential_reader(
             new rocksutil::SequentialFileReader(std::move(sequential_file)));

  return new rocksutil::log::Reader(std::move(sequential_reader), reporter,
            true, offset);
}

bool BinlogReader::TryToRollFile() {
  rocksutil::log::Reader* new_reader = CreateReader(env_,
      log_path_, number_ + 1, 0, &reporter_);
  if (new_reader != nullptr) {
    delete reader_;
    reader_ = new_reader;
    number_++;
    offset_ = 0;
    return true;
  }
  return false;
}

BinlogReader* CreateBinlogReader(const std::string& log_path,
    rocksutil::Env* env, uint64_t number, uint64_t offset) {
  
  BinlogReader* binlog_reader = new BinlogReader(nullptr, log_path, number,
                                      offset, env);

  rocksutil::log::Reader* reader = CreateReader(env,
      log_path, number, offset, binlog_reader->reporter());

  if (reader == nullptr) {
    delete binlog_reader;
    return nullptr;
  } else {
    binlog_reader->set_reader(reader);
    return binlog_reader;
  }
}
