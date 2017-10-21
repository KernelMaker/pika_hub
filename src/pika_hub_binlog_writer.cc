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

void BinlogWriter::WriteThread::LinkOne(Executor* e,
    bool* linked_as_leader) {
  while (true) {
    Executor* executor = newest_executor_.load(std::memory_order_relaxed);
    e->link_older = executor;
    if (newest_executor_.compare_exchange_strong(executor, e)) {
      *linked_as_leader = (executor == nullptr);
      return;
    }
  }
}

void BinlogWriter::WriteThread::JoinTaskGroup(Executor* e) {
  bool linked_as_leader;
  LinkOne(e, &linked_as_leader);

  if (!linked_as_leader) {
    rocksutil::MutexLock l(&e->mutex);
    while (!e->leader && !e->done) {
      e->cv.Wait();
    }
  } else {
    e->leader = true;
  }
}

void BinlogWriter::WriteThread::EnterAsTaskGroupLeader(
    Executor** newest_executor) {
  *newest_executor = newest_executor_.load(
      std::memory_order_acquire);
  Executor* head = *newest_executor;
  while (true) {
    Executor* next = head->link_older;
    if (next == nullptr || next->link_newer == head) {
      break;
    }
    next->link_newer = head;
    head = next;
  }
}

void BinlogWriter::WriteThread::ExitAsTaskGroupLeader(
    Executor* leader, Executor* last_executor,
    const rocksutil::Status& result) {
  Executor* head = newest_executor_.load(std::memory_order_acquire);
  if (head != last_executor ||
       !newest_executor_.compare_exchange_strong(head, nullptr)) {
    while (true) {
      Executor* next = head->link_older;
      if (next == nullptr || next->link_newer == head) {
        break;
      }
      next->link_newer = head;
      head = next;
    }

    {
    rocksutil::MutexLock l(&(last_executor->link_newer->mutex));
    last_executor->link_newer->link_older = nullptr;
    last_executor->link_newer->leader = true;
    }
    last_executor->link_newer->cv.SignalAll();
  }

  while (last_executor != leader) {
    rocksutil::MutexLock l(&(last_executor->mutex));
    last_executor->status = result;
    last_executor->done = true;
    last_executor->cv.SignalAll();
    Executor* next = last_executor->link_older;
    last_executor = next;
  }
}

uint64_t BinlogWriter::GetOffsetInFile() {
  return writer_->file()->GetFileSize();
}

rocksutil::Status BinlogWriter::Append(uint8_t op, const std::string& key,
    const std::string& value, int32_t server_id,
    int32_t exec_time) {
  Task task(op, key, value, server_id, exec_time);
  return Append(&task);
}

rocksutil::Status BinlogWriter::Append(Task* task) {
  Executor e(task);
  write_thread_.JoinTaskGroup(&e);
  if (!e.leader && e.done) {
    return e.status;
  }

  // only LEADER reaches this point
  assert(e.leader == true);

  if (GetOffsetInFile() >= kMaxBinlogFileSize) {
    RollFile();
  }

  count_++;
  assert(count_ == 1);

  Executor* newest_executor;
  write_thread_.EnterAsTaskGroupLeader(&newest_executor);

  Executor* last_executor = &e;
  std::string rep;
  while (true) {
    rocksutil::Cache::Handle* handle = manager_->lru_cache()->
      Lookup(last_executor->task->key_);
    bool valid = true;
    if (handle) {
      int32_t _exec_time = static_cast<CacheEntity*>(
          manager_->lru_cache()->Value(handle))->exec_time;
      if (last_executor->task->exec_time_ <= _exec_time) {
        valid = false;
      }
      manager_->lru_cache()->Release(handle);
    }
    if (valid) {
      CacheEntity* entity = new CacheEntity(last_executor->task->server_id_,
          last_executor->task->exec_time_);
      manager_->lru_cache()->Insert(last_executor->task->key_, entity,
          1, &CacheEntityDeleter);

      rep.append(last_executor->task->rep_);
    }

    if (last_executor == newest_executor) {
      break;
    }
    last_executor = last_executor->link_newer;
  }

  rocksutil::Status result;
  if (!rep.empty()) {
    {
    rocksutil::MutexLock l(manager_->mutex());
    result = writer_->AddRecord(rep);
    manager_->UpdateWriterOffset(number_, GetOffsetInFile());
    manager_->cv()->SignalAll();
    }
  }

  count_--;
  assert(count_ == 0);
  write_thread_.ExitAsTaskGroupLeader(&e, last_executor, result);

  e.done = true;
  return result;
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


void BinlogWriter::EncodeBinlogContent(std::string* result,
    uint8_t op, const std::string& key, const std::string& value,
    int32_t server_id, int32_t exec_time) {
  result->clear();

  result->append(reinterpret_cast<char*>(&op), sizeof(uint8_t));
  rocksutil::PutFixed32(result, server_id);
  rocksutil::PutFixed32(result, exec_time);
  rocksutil::PutFixed32(result, key.size());
  result->append(key.data(), key.size());
  rocksutil::PutFixed32(result, value.size());
  result->append(value.data(), value.size());
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
