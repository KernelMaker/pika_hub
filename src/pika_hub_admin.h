//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef SRC_PIKA_HUB_ADMIN_H_
#define SRC_PIKA_HUB_ADMIN_H_
#include "src/pika_hub_command.h"
#include "src/pika_hub_client_conn.h"

#include <string>

class PingCmd : public Cmd {
 public:
  PingCmd() {}
  virtual void Do() override;
 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) override;
};

class InfoCmd : public Cmd {
 public:
  InfoCmd() {}
  virtual void Do() override;

 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) override;
};

class TransferCmd : public Cmd {
 public:
  TransferCmd() {}
  virtual void Do() override;

 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) override;
  std::string server_id_;
  std::string new_ip_;
  std::string new_port_;
};

class CopyCmd : public Cmd {
 public:
  CopyCmd() {}
  virtual void Do() override;

 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) override;
  std::string src_server_id_;
  std::string new_server_id_;
  std::string new_ip_;
  std::string new_port_;
  std::string passwd_;
};

class AuthCmd : public Cmd {
 public:
  AuthCmd() {}
  virtual void Do() override;

 private:
  virtual void DoInitial(const PikaCmdArgsType &argvs,
      const CmdInfo* const ptr_info) override;
  std::string passwd_;
};
#endif  // SRC_PIKA_HUB_ADMIN_H_
