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

rocksutil::Status BinlogReader::ReadRecord(
    std::vector<BinlogFields>* result) {
  bool ret = true;
  uint64_t writer_number = 0;
  uint64_t writer_offset = 0;
  uint64_t reader_offset = 0;
  std::string scratch;
  rocksutil::Slice record;
  while (!should_exit_) {
    ret = reader_->ReadRecord(&record, &scratch,
        rocksutil::log::WALRecoveryMode::kAbsoluteConsistency);
    if (ret) {
      DecodeBinlogContent(record, result);
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
        uint64_t cur_file_size;
        env_->GetFileSize(log_path_ + "/" + kBinlogPrefix +
            std::to_string(number_), &cur_file_size);
        if (cur_file_size > reader_offset) {
          reader_->UnmarkEOF();
          manager_->mutex()->Unlock();
          continue;
        }
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
  return rocksutil::Status::Corruption("Exit");
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
    std::vector<BinlogFields>* result) {
  int32_t pos = 0;
  int32_t total = content.size();

  uint8_t op = 0;
  int32_t server_id = 0;
  int32_t exec_time = 0;
  int32_t key_size = 0;
  int32_t value_size = 0;

  result->clear();
  while (pos + 1 < total) {
    op = static_cast<uint8_t>(*(content.data() + pos));
    server_id = rocksutil::DecodeFixed32(content.data() + pos + 1);
    exec_time = rocksutil::DecodeFixed32(content.data() + pos + 5);
    key_size = rocksutil::DecodeFixed32(content.data() + pos + 9);
    value_size = rocksutil::DecodeFixed32(content.data() + pos
        + 13 + key_size);

    result->push_back({op, server_id, exec_time,
        std::string(content.data() + pos + 13, key_size),
        std::string(content.data() + pos + 17 + key_size, value_size)
        });

    pos += (17 + key_size + value_size);
  }
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
