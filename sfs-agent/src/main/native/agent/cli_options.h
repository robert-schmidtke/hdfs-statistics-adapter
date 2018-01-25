/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
#ifndef CLI_OPTIONS_H
#define CLI_OPTIONS_H

#include <cstdio>
#include <cstring>

// from the hprof agent, see attached LICENSE file
static int get_tok(char **src, char *buf, int buflen, int sep) {
  int len;
  char *p;

  buf[0] = 0;
  if (**src == 0) {
    return 0;
  }
  p = strchr(*src, sep);
  if (p == NULL) {
    len = (int)strlen(*src);
    p = (*src) + len;
  } else {
    /*LINTED*/
    len = (int)(p - (*src));
  }
  if ((len + 1) > buflen) {
    return 0;
  }
  (void)memcpy(buf, *src, len);
  buf[len] = 0;
  if (*p != 0 && *p == sep) {
    (*src) = p + 1;
  } else {
    (*src) = p;
  }
  return len;
}

struct CliOptions {
  std::string key;
  std::string transformer_address;
  std::string transformer_jar_path;
  std::string time_bin_duration;
  std::string time_bin_cache_size;
  std::string output_directory;
  std::string output_format;
  std::string instrumentation_skip;
  std::string operation_statistics_pool_size;
  std::string data_operation_statistics_pool_size;
  std::string read_data_operation_statistics_pool_size;
  std::string task_queue_size;
  std::string long_queue_lock_cache_size;
  std::string operation_statistics_lock_cache_size;
  bool trace_mmap;
  bool trace_fds;
  bool use_proxy;
  bool verbose;
};

// borrows from the hprof agent
static bool parse_options(char *command_line_options, CliOptions *cli_options) {
  bool delete_command_line_options = false;
  if (command_line_options == NULL) {
    command_line_options = new char[1];
    command_line_options[0] = 0;
    delete_command_line_options = true;
  }

  char *extra_options = getenv("_JAVA_SFS_OPTIONS");
  bool delete_extra_options = false;
  if (extra_options == NULL) {
    extra_options = new char[1];
    extra_options[0] = 0;
    delete_extra_options = true;
  }

  char *all_options = new char[(int)strlen(command_line_options) +
                               (int)strlen(extra_options) + 2];
  (void)strcpy(all_options, command_line_options);
  if (extra_options[0] != 0) {
    if (all_options[0] != 0) {
      (void)strcat(all_options, ",");
    }
    (void)strcat(all_options, extra_options);
  }
  char *options = all_options;

  cli_options->key = std::string("");
  cli_options->transformer_address = std::string("");
  cli_options->transformer_jar_path = std::string("");
  cli_options->time_bin_duration = std::string("");
  cli_options->time_bin_cache_size = std::string("");
  cli_options->output_directory = std::string("");
  cli_options->output_format = std::string("");
  cli_options->instrumentation_skip = std::string("");
  cli_options->trace_mmap = false;
  cli_options->trace_fds = false;
  cli_options->use_proxy = false;
  cli_options->verbose = false;

  bool tx_jar_path_set = false;
  bool key_set = false;
  bool time_bin_duration_set = false;
  bool time_bin_cache_size_set = false;
  bool output_directory_set = false;
  bool output_format_set = false;
  bool os_pool_set = false;
  bool dos_pool_set = false;
  bool rdos_pool_set = false;
  bool tq_pool_set = false;

  while (*options) {
    char option[16];
    char suboption[FILENAME_MAX + 1];

    if (get_tok(&options, option, (int)sizeof(option), '=')) {
      if (strcmp(option, "trans_address") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->transformer_address = std::string(suboption);
        }
      } else if (strcmp(option, "trans_jar") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->transformer_jar_path = std::string(suboption);
          tx_jar_path_set = true;
        }
      } else if (strcmp(option, "key") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->key = std::string(suboption);
          key_set = true;
        }
      } else if (strcmp(option, "bin_duration") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->time_bin_duration = std::string(suboption);
          time_bin_duration_set = true;
        }
      } else if (strcmp(option, "cache_size") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->time_bin_cache_size = std::string(suboption);
          time_bin_cache_size_set = true;
        }
      } else if (strcmp(option, "out_dir") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->output_directory = std::string(suboption);
          output_directory_set = true;
        }
      } else if (strcmp(option, "out_fmt") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->output_format = std::string(suboption);
          output_format_set = true;
        }
      } else if (strcmp(option, "os_pool_size") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->operation_statistics_pool_size = std::string(suboption);
          os_pool_set = true;
        }
      } else if (strcmp(option, "dos_pool_size") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->data_operation_statistics_pool_size =
              std::string(suboption);
          dos_pool_set = true;
        }
      } else if (strcmp(option, "rdos_pool_size") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->read_data_operation_statistics_pool_size =
              std::string(suboption);
          rdos_pool_set = true;
        }
      } else if (strcmp(option, "tq_pool_size") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->task_queue_size = std::string(suboption);
          tq_pool_set = true;
        }
      } else if (strcmp(option, "lq_lock_cache") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->long_queue_lock_cache_size = std::string(suboption);
        }
      } else if (strcmp(option, "os_lock_cache") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->operation_statistics_lock_cache_size =
              std::string(suboption);
        }
      } else if (strcmp(option, "instr_skip") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->instrumentation_skip = std::string(suboption);
        }
      } else if (strcmp(option, "trace_mmap") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->trace_mmap = strcmp(suboption, "y") == 0;
        }
      } else if (strcmp(option, "trace_fds") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->trace_fds = strcmp(suboption, "y") == 0;
        }
      } else if (strcmp(option, "use_proxy") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->use_proxy = strcmp(suboption, "y") == 0;
        }
      } else if (strcmp(option, "verbose") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->verbose = strcmp(suboption, "y") == 0;
        }
      }
    }
  }

  if (delete_command_line_options) {
    delete[] command_line_options;
  }
  if (delete_extra_options) {
    delete[] extra_options;
  }
  delete[] all_options;

  return tx_jar_path_set && key_set && time_bin_duration_set &&
         time_bin_cache_size_set && output_directory_set && output_format_set &&
         os_pool_set && dos_pool_set && rdos_pool_set && tq_pool_set;
}

#endif // CLI_OPTIONS_H
