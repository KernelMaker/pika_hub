//  Copyright (c) 2017-present The pika_hub Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <getopt.h>
#include <signal.h>

#include "src/pika_hub_server.h"
#include "src/pika_hub_conf.h"
#include "src/pika_hub_command.h"

PikaHubServer* g_pika_hub_server;
PikaHubConf* g_pika_hub_conf;

static void Usage() {
  fprintf(stderr,
      "usage: pika_hub [-h] [-c conf/file]\n"
      "\t-h               -- show this help\n"
      "\t-c conf/file     -- config file \n"
      "  example: ./output/bin/pika_hub -c ./conf/pika_hub.conf\n");
}

static void PikaHubConfInit(const std::string& path) {
  printf("path : %s\n", path.c_str());
  g_pika_hub_conf = new PikaHubConf(path);
  if (g_pika_hub_conf->Load() != 0) {
    fprintf(stderr, "pika_hub load conf error\n");
    exit(-1);
  }
  printf("-----------pika_hub config list----------\n");
  g_pika_hub_conf->DumpConf();
  printf("-----------pika_hub config end----------\n");
}


void IntSigHandle(int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  g_pika_hub_server->Exit();
}

void SignalSetup() {
  signal(SIGPIPE, SIG_IGN);
  if (signal(SIGINT, &IntSigHandle) == SIG_ERR) {
    printf("Catch SignalInt error\n");
  }
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

void CloseSTD() {
  int fd;
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, STDIN_FILENO);
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
    close(fd);
  }
}

void CreatePidFile(rocksutil::Env* env) {
  /* Try to write the pid file in a best-effort way. */
  std::string path(g_pika_hub_conf->pidfile());

  size_t pos = path.find_last_of('/');
  if (pos != std::string::npos) {
    // mkpath(path.substr(0, pos).c_str(), 0755);
    env->CreateDir(path.substr(0, pos));
  } else {
    path = "pika_hub.pid";
  }

  FILE *fp = fopen(path.c_str(), "w");
  if (fp) {
    fprintf(fp, "%d\n", static_cast<int>(getpid()));
    fclose(fp);
  }
}

int main(int argc, char** argv) {
  if (argc < 2) {
    Usage();
    exit(-1);
  }

  bool path_opt = false;
  char c;
  char path[1024];
  while (-1 != (c = getopt(argc, argv, "c:h"))) {
    switch (c) {
      case 'c':
        snprintf(path, sizeof(path), "%s", optarg);
        path_opt = true;
        break;
      case 'h':
        Usage();
        return 0;
      default:
        Usage();
        return 0;
    }
  }

  if (path_opt == false) {
    fprintf(stderr, "Please specify the conf file path\n");
    Usage();
    exit(-1);
  }

  PikaHubConfInit(path);

  Options options;
  options.str_members = g_pika_hub_conf->floyd_servers();
  options.local_ip = g_pika_hub_conf->floyd_local_ip();
  options.local_port = g_pika_hub_conf->floyd_local_port();
  options.path = g_pika_hub_conf->floyd_path();
  options.port = g_pika_hub_conf->sdk_port();
  options.info_log_path = g_pika_hub_conf->log_path();
  options.max_log_file_size = g_pika_hub_conf->max_log_file_size();
  options.log_file_time_to_roll = g_pika_hub_conf->log_file_time_to_roll();
  options.info_log_level = static_cast<rocksutil::InfoLogLevel>(
      g_pika_hub_conf->info_log_level());
  options.pika_servers = g_pika_hub_conf->pika_servers();

  SignalSetup();
  InitCmdInfoTable();

  g_pika_hub_server = new PikaHubServer(options);
  g_pika_hub_server->DumpOptions();

  if (g_pika_hub_conf->daemonize()) {
    if (fork() != 0) {
      exit(0); /* parent exits */
    }
    setsid(); /* create a new session */
    CreatePidFile(options.env);
    CloseSTD();
  }

  slash::Status s = g_pika_hub_server->Start();
  if (!s.ok()) {
    printf("Start Server error\n");
    return -1;
  }

  if (g_pika_hub_conf->daemonize()) {
    unlink(g_pika_hub_conf->pidfile().c_str());
  }

  printf("pika_hub exit...\n");

  return 0;
}
