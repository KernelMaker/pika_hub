#ifndef PIKA_HUB_BINLOG_READER_H_
#define PIKA_HUB_BINLOG_READER_H_

#include "rocksutil/log_reader.h"
#include "rocksutil/env.h"
class BinlogManager;
class BinlogReader {
 public:
   BinlogReader(rocksutil::log::Reader* reader,
      const std::string& log_path, 
      uint64_t number, uint64_t offset,
      rocksutil::Env* env,
      BinlogManager* manager)
   : reader_(reader), log_path_(log_path),
   number_(number), offset_(offset),
   env_(env), manager_(manager),
   should_exit_(false) {
     reporter_.status = &status_;
   };

   ~BinlogReader() {
     delete reader_;
   }

   rocksutil::Status ReadRecord(rocksutil::Slice* slice, std::string* scratch);
   bool IsEOF() {
     return reader_->IsEOF();
   }
   void set_reader(rocksutil::log::Reader* reader) {
     reader_ = reader;
   }
   rocksutil::log::Reader::LogReporter* reporter() {
     return &reporter_;
   };

   void StopRead();
   
 private:
   bool TryToRollFile();
   rocksutil::log::Reader* reader_;
   std::string log_path_;
   uint64_t number_;
   uint64_t offset_;
   rocksutil::Env* env_;
   BinlogManager* manager_;
   bool should_exit_;
   rocksutil::Status status_;
   rocksutil::log::Reader::LogReporter reporter_;
};

extern BinlogReader* CreateBinlogReader(const std::string& log_path,
    rocksutil::Env* env, uint64_t number, uint64_t offset,
    BinlogManager* manager);
  
#endif
