//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/pika_hub_binlog_writer.h"

#include <utility>
#include <memory>
#include <string>

#include "src/pika_hub_common.h"
#include "src/pika_hub_binlog_manager.h"
#include "rocksutil/file_reader_writer.h"
#include "rocksutil/coding.h"

uint64_t BinlogWriter::GetOffsetInFile() {
  return writer_->file()->GetFileSize();
}

rocksutil::Status BinlogWriter::Append(uint8_t op, const std::string& key,
    const std::string& value, int32_t server_id,
    int32_t exec_time) {
  rocksutil::MutexLock l(manager_->mutex());
  if (GetOffsetInFile() >= kMaxBinlogFileSize) {
    RollFile();
  }

  rocksutil::Cache::Handle* handle = manager_->lru_cache()->Lookup(key);
  if (handle) {
    int32_t _exec_time = static_cast<CacheEntity*>(
        manager_->lru_cache()->Value(handle))->exec_time;
    if (exec_time <= _exec_time) {
      return rocksutil::Status::OK();
    }
  }
  CacheEntity* entity = new CacheEntity(server_id, exec_time);
  manager_->lru_cache()->Insert(key, entity, 1, &CacheEntityDeleter);

  std::string str = EncodeBinlogContent(op, key, value,
      server_id, exec_time);

  rocksutil::Status s = writer_->AddRecord(str);
  manager_->UpdateWriterOffset(number_, GetOffsetInFile());
  manager_->cv()->SignalAll();
  return s;
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
       new rocksutil::WritableFileWriter(std::move(writable_file),
         env_options));

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

void BinlogWriter::CacheEntityDeleter(const rocksutil::Slice& key,
    void* value) {
  delete static_cast<CacheEntity*>(value);
}


std::string BinlogWriter::EncodeBinlogContent(uint8_t op,
    const std::string& key, const std::string& value,
    int32_t server_id, int32_t exec_time) {
  std::string result;
  result.clear();

  result.append(reinterpret_cast<char*>(&op), sizeof(uint8_t));
  rocksutil::PutFixed32(&result, server_id);
  rocksutil::PutFixed32(&result, exec_time);
  rocksutil::PutFixed32(&result, key.size());
  result.append(key.data(), key.size());
  result.append(value.data(), value.size());

  return result;
}


BinlogWriter* CreateBinlogWriter(const std::string& log_path,
    uint64_t number, rocksutil::Env* env,
    BinlogManager* manager) {
  rocksutil::log::Writer* writer = CreateWriter(env,
      log_path, number);
  return (writer == nullptr ?
      nullptr : new BinlogWriter(writer, number, log_path, env,
                      manager));
}
