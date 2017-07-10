//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef SRC_PIKA_HUB_COMMAND_H_
#define SRC_PIKA_HUB_COMMAND_H_

#include <deque>
#include <string>
#include <memory>
#include <unordered_map>

#include "slash/include/slash_string.h"
#include "pink/include/redis_conn.h"


//  Constant for command name
const char kCmdNamePing[] = "ping";
const char kCmdNameInfo[] = "info";

//  Sync command
const char kCmdNameSet[] = "set";

typedef pink::RedisCmdArgsType PikaCmdArgsType;

enum CmdFlagsMask {
  kCmdFlagsMaskRW               = 1,
  kCmdFlagsMaskType             = 28,
  kCmdFlagsMaskLocal            = 32,
  kCmdFlagsMaskSuspend          = 64,
  kCmdFlagsMaskPrior            = 128,
  kCmdFlagsMaskAdminRequire     = 256
};

enum CmdFlags {
  kCmdFlagsRead           = 0,  // default rw
  kCmdFlagsWrite          = 1,
  kCmdFlagsAdmin          = 0,  // default type
  kCmdFlagsKv             = 2,
  kCmdFlagsHash           = 4,
  kCmdFlagsList           = 6,
  kCmdFlagsSet            = 8,
  kCmdFlagsZset           = 10,
  kCmdFlagsBit            = 12,
  kCmdFlagsHyperLogLog    = 14,
  kCmdFlagsGeo            = 16,
  kCmdFlagsNoLocal        = 0,  // default nolocal
  kCmdFlagsLocal          = 32,
  kCmdFlagsNoSuspend      = 0,  // default nosuspend
  kCmdFlagsSuspend        = 64,
  kCmdFlagsNoPrior        = 0,  // default noprior
  kCmdFlagsPrior          = 128,
  kCmdFlagsNoAdminRequire = 0,  // default no need admin
  kCmdFlagsAdminRequire   = 256
};


class CmdInfo {
 public:
  CmdInfo(const std::string _name, int _num, uint16_t _flag)
    : name_(_name), arity_(_num), flag_(_flag) {}
  bool CheckArg(int num) const {
    if ((arity_ > 0 && num != arity_) || (arity_ < 0 && num < -arity_)) {
      return false;
    }
    return true;
  }
  bool is_write() const {
    return ((flag_ & kCmdFlagsMaskRW) == kCmdFlagsWrite);
  }
  uint16_t flag_type() const {
    return flag_ & kCmdFlagsMaskType;
  }
  bool is_local() const {
    return ((flag_ & kCmdFlagsMaskLocal) == kCmdFlagsLocal);
  }
  // Others need to be suspended when a suspend command run
  bool is_suspend() const {
    return ((flag_ & kCmdFlagsMaskSuspend) == kCmdFlagsSuspend);
  }
  bool is_prior() const {
    return ((flag_ & kCmdFlagsMaskPrior) == kCmdFlagsPrior);
  }
  // Must with admin auth
  bool is_admin_require() const {
    return ((flag_ & kCmdFlagsMaskAdminRequire) == kCmdFlagsAdminRequire);
  }
  std::string name() const {
    return name_;
  }

 private:
  std::string name_;
  int arity_;
  uint16_t flag_;

  CmdInfo(const CmdInfo&);
  CmdInfo& operator=(const CmdInfo&);
};

void inline RedisAppendContent(std::string* str, const std::string& value);
void inline RedisAppendLen(std::string* str,
    int ori, const std::string &prefix);

const char kNewLine[] = "\r\n";

class CmdRes {
 public:
  enum CmdRet {
    kNone = 0,
    kOk,
    kPong,
    kSyntaxErr,
    kInvalidInt,
    kInvalidBitInt,
    kInvalidBitOffsetInt,
    kInvalidBitPosArgument,
    kWrongBitOpNotNum,
    kInvalidFloat,
    kOverFlow,
    kNotFound,
    kOutOfRange,
    kInvalidPwd,
    kNoneBgsave,
    kPurgeExist,
    kInvalidParameter,
    kWrongNum,
    kInvalidIndex,
    kInvalidMagic,
    kErrOther,
  };

  CmdRes():ret_(kNone) {}

  bool none() const {
    return ret_ == kNone && message_.empty();
  }
  bool ok() const {
    return ret_ == kOk || ret_ == kNone;
  }
  void clear() {
    message_.clear();
    ret_ = kNone;
  }
  std::string raw_message() const {
    return message_;
  }
  std::string message() const {
    std::string result;
    switch (ret_) {
    case kNone:
      return message_;
    case kOk:
      return "+OK\r\n";
    case kPong:
      return "+PONG\r\n";
    case kSyntaxErr:
      return "-ERR syntax error\r\n";
    case kInvalidInt:
      return "-ERR value is not an integer or out of range\r\n";
    case kInvalidBitInt:
      return "-ERR bit is not an integer or out of range\r\n";
    case kInvalidBitOffsetInt:
      return "-ERR bit offset is not an integer or out of range\r\n";
    case kWrongBitOpNotNum:
      return "-ERR BITOP NOT must be called with a single source key.\r\n";

    case kInvalidBitPosArgument:
      return "-ERR The bit argument must be 1 or 0.\r\n";
    case kInvalidFloat:
      return "-ERR value is not an float\r\n";
    case kOverFlow:
      return "-ERR increment or decrement would overflow\r\n";
    case kNotFound:
      return "-ERR no such key\r\n";
    case kOutOfRange:
      return "-ERR index out of range\r\n";
    case kInvalidPwd:
      return "-ERR invalid password\r\n";
    case kNoneBgsave:
      return "-ERR No BGSave Works now\r\n";
    case kPurgeExist:
      return "-ERR binlog already in purging...\r\n";
    case kInvalidParameter:
      return "-ERR Invalid Argument\r\n";
    case kWrongNum:
      result = "-ERR wrong number of arguments for '";
      result.append(message_);
      result.append("' command\r\n");
      break;
    case kInvalidIndex:
      result = "-ERR invalid DB index\r\n";
      break;
    case kInvalidMagic:
      result = "-ERR invalid magic number\r\n";
      break;
    case kErrOther:
      result = "-ERR ";
      result.append(message_);
      result.append(kNewLine);
      break;
    default:
      break;
    }
    return result;
  }

  // Inline functions for Create Redis protocol
  void AppendStringLen(int ori) {
    RedisAppendLen(&message_, ori, "$");
  }
  void AppendArrayLen(int ori) {
    RedisAppendLen(&message_, ori, "*");
  }
  void AppendInteger(int ori) {
    RedisAppendLen(&message_, ori, ":");
  }
  void AppendContent(const std::string &value) {
    RedisAppendContent(&message_, value);
  }
  void AppendString(const std::string &value) {
    AppendStringLen(value.size());
    AppendContent(value);
  }
  void AppendStringRaw(const std::string &value) {
    message_.append(value);
  }
  void SetRes(CmdRet _ret, const std::string content = "") {
    ret_ = _ret;
    if (!content.empty()) {
      message_ = content;
    }
  }

 private:
  std::string message_;
  CmdRet ret_;
};

class Cmd {
 public:
  Cmd() {}
  virtual ~Cmd() {}
  virtual void Do() = 0;
  void Initial(const PikaCmdArgsType &argvs, const CmdInfo* const ptr_info) {
    res_.clear();  // Clear res content
    Clear();       // Clear cmd, Derived class can has own implement
    DoInitial(argvs, ptr_info);
  }
  CmdRes& res() {
    return res_;
  }

 protected:
  CmdRes res_;

 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) = 0;
  virtual void Clear() {}
  Cmd(const Cmd&);
  Cmd& operator=(const Cmd&);
};

typedef std::unordered_map<std::string, Cmd*> CmdTable;

// Method for CmdInfo Table
void InitCmdInfoTable();
const CmdInfo* GetCmdInfo(const std::string& opt);
void DestoryCmdInfoTable();

// Method for Cmd Table
void InitCmdTable(CmdTable* cmd_table);
Cmd* GetCmdFromTable(const std::string& opt, const CmdTable& cmd_table);
void DestoryCmdTable(CmdTable* cmd_table);

void inline RedisAppendContent(std::string* str, const std::string& value) {
  str->append(value.data(), value.size());
  str->append(kNewLine);
}
void inline RedisAppendLen(std::string* str, int ori,
    const std::string &prefix) {
  char buf[32];
  slash::ll2string(buf, 32, static_cast<int64_t>(ori));
  str->append(prefix);
  str->append(buf);
  str->append(kNewLine);
}
#endif  // SRC_PIKA_HUB_COMMAND_H_
