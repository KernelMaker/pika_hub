//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include <utility>
#include <memory>
#include <string>

#include "src/pika_hub_binlog_reader.h"
#include "src/pika_hub_common.h"
#include "src/pika_hub_binlog_manager.h"
#include "rocksutil/file_reader_writer.h"

void BinlogReader::StopRead() {
  should_exit_ = true;
  manager_->cv()->SignalAll();
}

rocksutil::Status BinlogReader::ReadRecord(rocksutil::Slice* slice,
    std::string* scratch) {
  bool ret = true;
  uint64_t writer_number = 0;
  uint64_t writer_offset = 0;
  uint64_t reader_offset = 0;
  while (true) {
    ret = reader_->ReadRecord(slice, scratch);
    if (ret) {
      return rocksutil::Status::OK();
    } else {
      if (status_.ok()) {
        manager_->mutex()->Lock();
        manager_->GetWriterOffset(&writer_number, &writer_offset);
        reader_offset = reader_->EndOfBufferOffset();
        std::cout << writer_number << " " << writer_offset << " " <<
          number_ << " " << reader_offset << std::endl;
        while (number_ == writer_number && reader_offset == writer_offset) {
          manager_->cv()->Wait();
          if (should_exit_) {
            manager_->mutex()->Unlock();
            return rocksutil::Status::Corruption("Exit");
          }
          manager_->GetWriterOffset(&writer_number, &writer_offset);
          reader_offset = reader_->EndOfBufferOffset();
        }
        reader_->UnmarkEOF();
        manager_->mutex()->Unlock();
        rocksutil::Status s = env_->FileExists(log_path_ + "/" + kBinlogPrefix +
              std::to_string(number_+1));
        if (s.ok()) {
          TryToRollFile();
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
    return true;
  }
  return false;
}

BinlogReader* CreateBinlogReader(const std::string& log_path,
    rocksutil::Env* env, uint64_t number, uint64_t offset,
    BinlogManager* manager) {

  BinlogReader* binlog_reader = new BinlogReader(nullptr, log_path, number,
                                      env, manager);

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
