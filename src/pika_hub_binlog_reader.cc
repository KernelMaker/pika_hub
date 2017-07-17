//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <memory>
#include <string>

#include "src/pika_hub_binlog_reader.h"
#include "src/pika_hub_common.h"
#include "src/pika_hub_binlog_manager.h"
#include "rocksutil/file_reader_writer.h"
#include "rocksutil/coding.h"

void BinlogReader::GetOffset(uint64_t* number, uint64_t* offset) {
  *number = number_;
  *offset = reader_->EndOfBufferOffset();
}

void BinlogReader::StopRead() {
  should_exit_ = true;
  manager_->cv()->SignalAll();
}

rocksutil::Status BinlogReader::ReadRecord(uint8_t* op,
    std::string* key, std::string* value,
    int32_t* server_id, int32_t* exec_time) {
  bool ret = true;
  uint64_t writer_number = 0;
  uint64_t writer_offset = 0;
  uint64_t reader_offset = 0;
  std::string scratch;
  rocksutil::Slice record;
  while (true) {
    ret = reader_->ReadRecord(&record, &scratch);
    if (ret) {
      DecodeBinlogContent(record, op, key, value, server_id, exec_time);
      return rocksutil::Status::OK();
    } else {
      if (status_.ok()) {
        manager_->mutex()->Lock();
        manager_->GetWriterOffset(&writer_number, &writer_offset);
        reader_offset = reader_->EndOfBufferOffset();
        while (number_ == writer_number && reader_offset == writer_offset) {
          /*
           * if exit_at_end_ is TRUE,
           * return directly when all the content have been read
           * this mode is used in BinlogManager lru recovering
           */
          if (exit_at_end_) {
            manager_->mutex()->Unlock();
            return rocksutil::Status::Corruption("Exit");
          }
          /*
           * if exit_at_end_ is FALSE
           * wait until new content is written or should exit;
           */
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

void BinlogReader::DecodeBinlogContent(const rocksutil::Slice& content,
    uint8_t* op, std::string* key, std::string* value,
    int32_t* server_id, int32_t* exec_time) {
  *op = static_cast<uint8_t>(content.data()[0]);
  *server_id = rocksutil::DecodeFixed32(content.data() + 1);
  *exec_time = rocksutil::DecodeFixed32(content.data() + 5);
  int32_t key_size = rocksutil::DecodeFixed32(content.data() + 9);
  *key = std::string(content.data() + 13, key_size);
  value->clear();
  *value = std::string(content.data() + 13 + key_size,
      content.size() - 13 - key_size);
}

BinlogReader* CreateBinlogReader(const std::string& log_path,
    rocksutil::Env* env, uint64_t number, uint64_t offset,
    BinlogManager* manager, bool ret_at_end) {

  BinlogReader* binlog_reader = new BinlogReader(nullptr, log_path, number,
                                      env, manager, ret_at_end);

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
