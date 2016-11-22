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
  std::string transformer_jar_path;
  int communication_port_agent, communication_port_transformer;
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

  bool tx_jar_path_set = false;
  bool comm_port_agent_set = false, comm_port_trans_set = false;
  while (*options) {
    char option[16];
    char suboption[FILENAME_MAX + 1];
    char *endptr;

    if (get_tok(&options, option, (int)sizeof(option), '=')) {
      if (strcmp(option, "trans_jar") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->transformer_jar_path = std::string(suboption);
          tx_jar_path_set = true;
        }
      } else if (strcmp(option, "comm_port_agent") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->communication_port_agent =
              (int)strtol(suboption, &endptr, 10);
          if ((endptr == NULL || *endptr == 0) &&
              cli_options->communication_port_agent > 0) {
            comm_port_agent_set = true;
          }
        }
      } else if (strcmp(option, "comm_port_trans") == 0) {
        if (get_tok(&options, suboption, (int)sizeof(suboption), ',')) {
          cli_options->communication_port_transformer =
              (int)strtol(suboption, &endptr, 10);
          if ((endptr == NULL || *endptr == 0) &&
              cli_options->communication_port_transformer > 0) {
            comm_port_trans_set = true;
          }
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

  return tx_jar_path_set && comm_port_agent_set && comm_port_trans_set;
}

#endif // CLI_OPTIONS_H
