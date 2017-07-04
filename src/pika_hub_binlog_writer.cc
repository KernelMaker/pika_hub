#include "pika_hub_binlog_writer.h"
#include "pika_hub_common.h"
#include "rocksutil/file_reader_writer.h"

uint64_t BinlogWriter::GetOffsetInFile() {
   return writer_->file()->GetFileSize();
}

rocksutil::Status BinlogWriter::Append(const std::string& str) {
  /*
   * thread safe is guaranteed by the caller (BinlogManager)
   */
  if (GetOffsetInFile() >= kMaxBinlogFileSize) {
    RollFile();
  }
  return writer_->AddRecord(str);
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
    uint64_t number, rocksutil::Env* env) {
  rocksutil::log::Writer* writer = CreateWriter(env,
      log_path, number);
  return (writer == nullptr ?
      nullptr : new BinlogWriter(writer, number, log_path, env));
}
