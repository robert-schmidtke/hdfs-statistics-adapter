/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
#include <jvmti.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <iostream>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "class_transformation_client.h"
#include "class_transformation_server.h"
#include "cli_options.h"
#include "error_macros.h"

// global handles to the client/server that communicate with the Java class
// bytecode transformer JVM
static ClassTransformationClient *g_class_transformation_client;
static ClassTransformationServer *g_class_transformation_server;

// flag to indicate whether we should start our own transformer JVM
static bool g_start_transformer_jvm;

// store startup command of transformer JVM so we can clean it up later
static char **g_transformer_jvm_cmd;

// the prefix to use when wrapping native methods
static std::string g_native_method_prefix("sfs_native_");

// a custom key to include in the log files
static std::string g_key;

// time in nanoseconds to aggregate incoming events over before beginning a new
// bin
static std::string g_time_bin_duration;

// number of bins to keep in memory before emitting them to allow for late
// arrivals of log events
static std::string g_time_bin_cache_size;

// path to a directory where the output files will be stored
static std::string g_output_directory;

// format of the generated output: csv, fb, bb
static std::string g_output_format;

// off-heap data structure sizes, as no. of elements in them
static std::string g_os_pool_size;
static std::string g_dos_pool_size;
static std::string g_rdos_pool_size;
static std::string g_tq_pool_size;

// number of locks to cache for some types
static std::string g_lq_lock_cache_size;
static std::string g_mp_lock_cache_size;

// if set, mmap operation statistics to this directory instead of keeping them
// all in memory
static std::string g_mmap_directory;

// whether to trace mmap calls as well
static bool g_trace_mmap = false;

// whether to trace calls on a per file basis or globally
static bool g_trace_fds = false;

// indicates whether we should do verbose logging
static bool g_verbose = false;
#define LOG_VERBOSE(...)                                                       \
  do {                                                                         \
    if (g_verbose) {                                                           \
      fprintf(stderr, __VA_ARGS__);                                            \
    }                                                                          \
  } while (false)

// performs deregistration of events, server shutdown and memory freeing
static void cleanup();

// function to be called for each class loaded by the JVM
static void JNICALL ClassFileLoadHookCallback(jvmtiEnv *, JNIEnv *, jclass,
                                              jobject, const char *, jobject,
                                              jint, const unsigned char *,
                                              jint *, unsigned char **);

// function to be called when the JVM initializes
static void JNICALL VMInitCallback(jvmtiEnv *, JNIEnv *, jthread);

// function to be called when the JVM shuts down
static void JNICALL VMDeathCallback(jvmtiEnv *, JNIEnv *);

// called by the JVM to load the agent
JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *vm, char *options, void *reserved) {
  // parse command line options
  CliOptions cli_options;
  if (!parse_options(options, &cli_options)) {
    std::cerr
        << "Could not parse options: " << (options != NULL ? options : "-")
        << std::endl
        << "Required options:" << std::endl
        << "  trans_jar=/path/to/trans.jar" << std::endl
        << "  key=key" << std::endl
        << "  bin_duration=nanoseconds" << std::endl
        << "  cache_size=number" << std::endl
        << "  out_dir=/path/to/out/dir" << std::endl
        << "  out_fmt=csv|fb|bb" << std::endl
        << "  os_pool_size=number" << std::endl
        << "  dos_pool_size=number" << std::endl
        << "  rdos_pool_size=number" << std::endl
        << "  tq_pool_size=number" << std::endl
        << "Optional options:" << std::endl
        << "  instr_skip=r|w|o|z or any combination of them (default: empty)"
        << std::endl
        << "  trans_address=trans-host:port (default: empty)"
        << std::endl
        // default in LongQueue.java
        << "  lq_lock_cache=number (default: 1024)"
        << std::endl
        // default in MemoryPool.java
        << "  mp_lock_cache=number (default: 1024)" << std::endl
        << "  mmap_dir=/path/to/dir (default: empty)" << std::endl
        << "  trace_mmap=y|n (default: n)" << std::endl
        << "  trace_fds=y|n (default: n)" << std::endl
        << "  use_proxy=y|n (default: n)" << std::endl
        << "  verbose=y|n (default: n)" << std::endl;
    return JNI_EINVAL;
  }

  g_verbose = cli_options.verbose;
  LOG_VERBOSE("Agent loading.\n");

  // make sure a valid JAVA_HOME is set
  if (getenv("JAVA_HOME") == NULL) {
    std::cerr << "JAVA_HOME not set." << std::endl;
    return JNI_EINVAL;
  }

  // we append the slash later on
  std::string java_home(getenv("JAVA_HOME"));
  if (java_home.back() == '/') {
    java_home.pop_back();
  }
  LOG_VERBOSE("JAVA_HOME='%s'.\n", java_home.c_str());

  // variables used for error checking
  jint jni_result = JNI_OK;
  jvmtiError jvmti_result = JVMTI_ERROR_NONE;

  // obtain the JVMTI environment
  LOG_VERBOSE("Getting JVM TI environment.\n");
  jvmtiEnv *jvmti = NULL;
  jni_result = vm->GetEnv((void **)&jvmti, JVMTI_VERSION);
  CHECK_JNI_RESULT("GetEnv", jni_result);

  // register required capabilities:
  // - get notified upon class definitions
  // - allow rewriting their bytecodes and signatures
  LOG_VERBOSE("Registering capabilities.\n");
  jvmtiCapabilities capabilities;
  (void)memset(&capabilities, 0, sizeof(jvmtiCapabilities));
  capabilities.can_generate_all_class_hook_events = 1;
  capabilities.can_retransform_any_class = 1;
  capabilities.can_retransform_classes = 1;
  capabilities.can_set_native_method_prefix = 1;
  jvmti_result = jvmti->AddCapabilities(&capabilities);
  CHECK_JVMTI_RESULT("AddCapabilities", jvmti_result);

  // register callback to be called when classes are loaded
  LOG_VERBOSE("Setting event callbacks.\n");
  jvmtiEventCallbacks eventCallbacks;
  (void)memset(&eventCallbacks, 0, sizeof(eventCallbacks));
  eventCallbacks.ClassFileLoadHook = &ClassFileLoadHookCallback;
  eventCallbacks.VMInit = &VMInitCallback;
  eventCallbacks.VMDeath = &VMDeathCallback;
  jvmti_result =
      jvmti->SetEventCallbacks(&eventCallbacks, (jint)sizeof(eventCallbacks));
  CHECK_JVMTI_RESULT("SetEventCallbacks", jvmti_result);

  // enable notification sending on class loading
  LOG_VERBOSE("Setting event notifications.\n");
  jvmti_result = jvmti->SetEventNotificationMode(
      JVMTI_ENABLE, JVMTI_EVENT_CLASS_FILE_LOAD_HOOK, (jthread)NULL);
  CHECK_JVMTI_RESULT("SetEventNotificationMode(ClassFileLoadHook)",
                     jvmti_result);
  jvmti_result = jvmti->SetEventNotificationMode(
      JVMTI_ENABLE, JVMTI_EVENT_VM_INIT, (jthread)NULL);
  CHECK_JVMTI_RESULT("SetEventNotificationMode(VMInit)", jvmti_result);
  jvmti_result = jvmti->SetEventNotificationMode(
      JVMTI_ENABLE, JVMTI_EVENT_VM_DEATH, (jthread)NULL);
  CHECK_JVMTI_RESULT("SetEventNotificationMode(VMDeath)", jvmti_result);

  // set necessary global variables from CLI options
  g_key = cli_options.key;
  g_time_bin_duration = cli_options.time_bin_duration;
  g_time_bin_cache_size = cli_options.time_bin_cache_size;
  g_output_directory = cli_options.output_directory;
  g_output_format = cli_options.output_format;
  g_os_pool_size = cli_options.operation_statistics_pool_size;
  g_dos_pool_size = cli_options.data_operation_statistics_pool_size;
  g_rdos_pool_size = cli_options.read_data_operation_statistics_pool_size;
  g_tq_pool_size = cli_options.task_queue_size;
  g_lq_lock_cache_size = cli_options.long_queue_lock_cache_size;
  g_mp_lock_cache_size = cli_options.memory_pool_lock_cache_size;
  g_mmap_directory = cli_options.mmap_directory;
  g_trace_mmap = cli_options.trace_mmap;
  g_trace_fds = cli_options.trace_fds;

  // set the prefix to use when wrapping native methods
  LOG_VERBOSE("Setting native method prefix.\n");
  jvmti_result = jvmti->SetNativeMethodPrefix(g_native_method_prefix.c_str());
  CHECK_JVMTI_RESULT("SetNativeMethodPrefix", jvmti_result);

  // make all necessary classes known to this JVM (especially logging)
  LOG_VERBOSE("Adding agent jar to classpaths.\n");
  jvmti_result = jvmti->AddToBootstrapClassLoaderSearch(
      cli_options.transformer_jar_path.c_str());
  CHECK_JVMTI_RESULT("AddToBootstrapClassLoaderSearch", jvmti_result);
  jvmti_result = jvmti->AddToSystemClassLoaderSearch(
      cli_options.transformer_jar_path.c_str());
  CHECK_JVMTI_RESULT("AddToSystemClassLoaderSearch", jvmti_result);

  // disable http_proxy environment variable if specified
  if (!cli_options.use_proxy) {
    LOG_VERBOSE("Disabling http_proxy.\n");
    unsetenv("http_proxy");
  }

  // enable verbose logging for gRPC if requested
  if (g_verbose) {
    char grpc_verbosity[] = {"GRPC_VERBOSITY=DEBUG,GRPC_TRACE=all"};
    putenv(grpc_verbosity);
  }

  // figure out whether we should start our own transformer JVM or use an
  // already running one
  g_start_transformer_jvm = cli_options.transformer_address.length() == 0;
  if (g_start_transformer_jvm) {
    LOG_VERBOSE("Starting transformer JVM.\n");

    // start the server that communicates with the transformer JVM, i.e. waits
    // for it to have started
    g_class_transformation_server = new ClassTransformationServer;

    // assume that all that can go wrong during startup is a port that is
    // already in use
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    srand((time_t)ts.tv_nsec);

    int port, tries = 0;
    bool started = false;
    do {
      ++tries;
      port = rand() % 16384 + 49152;
      LOG_VERBOSE("Trying to start agent transformation server on port '%d'.\n",
                  port);
      started = g_class_transformation_server->Start("0.0.0.0:" +
                                                     std::to_string(port));
    } while (!started && tries < 10);
    if (!started) {
      std::cerr << "Could not start transformation server after " << tries
                << " tries." << std::endl;
      cleanup();
      return JNI_ERR;
    }
    LOG_VERBOSE("Started agent transformation server on port '%d'.\n", port);

    // build the transformer JVM start command
    g_transformer_jvm_cmd = new char *[13];
    g_transformer_jvm_cmd[0] = strdup((java_home + "/bin/java").c_str());
    g_transformer_jvm_cmd[1] = strdup("-cp");
    g_transformer_jvm_cmd[2] = strdup(cli_options.transformer_jar_path.c_str());
    g_transformer_jvm_cmd[3] =
        strdup("de.zib.sfs.instrument.ClassTransformationService");
    g_transformer_jvm_cmd[4] = strdup("--instrumentation-skip");
    g_transformer_jvm_cmd[5] = strdup(cli_options.instrumentation_skip.c_str());
    g_transformer_jvm_cmd[6] = strdup("--communication-port-agent");
    g_transformer_jvm_cmd[7] = strdup(std::to_string(port).c_str());
    g_transformer_jvm_cmd[8] = strdup("--trace-mmap");
    g_transformer_jvm_cmd[9] = strdup(g_trace_mmap ? "y" : "n");
    g_transformer_jvm_cmd[10] = strdup("--verbose");
    g_transformer_jvm_cmd[11] = strdup(g_verbose ? "y" : "n");
    g_transformer_jvm_cmd[12] = NULL;
    LOG_VERBOSE("Starting transformer JVM using command '%s %s %s %s %s %s %s "
                "%s %s %s %s %s'.\n",
                g_transformer_jvm_cmd[0], g_transformer_jvm_cmd[1],
                g_transformer_jvm_cmd[2], g_transformer_jvm_cmd[3],
                g_transformer_jvm_cmd[4], g_transformer_jvm_cmd[5],
                g_transformer_jvm_cmd[6], g_transformer_jvm_cmd[7],
                g_transformer_jvm_cmd[8], g_transformer_jvm_cmd[9],
                g_transformer_jvm_cmd[10], g_transformer_jvm_cmd[11]);

    char *envp[] = {NULL};

    // pipe for redirecting transformer JVM's output
    int pipe_fds[2];
    if (pipe(pipe_fds) != 0) {
      int errnum = errno;
      std::cerr << "pipe returned " << std::strerror(errnum) << ": " << errnum
                << std::endl;
      cleanup();
      return JNI_ERR;
    }

    // write end of pipe to stderr of parent
    dup2(2, pipe_fds[1]);

    // no need for read end of pipe
    close(pipe_fds[0]);

    // start new process to execute the transformer JVM
    pid_t transformer_pid = fork();
    if (transformer_pid == 0) {
      /* this block is executed by the child thread */

      // stdout/stderr of child to write end of pipe
      dup2(pipe_fds[1], 1);
      dup2(pipe_fds[1], 2);

      // start the transformer JVM, execve does not return on success
      execve((java_home + "/bin/java").c_str(), g_transformer_jvm_cmd, envp);
      int errnum = errno;
      std::cerr << "Could not start transformer JVM (" << std::strerror(errnum)
                << ": " << errnum << ")" << std::endl;
      return errnum;
    } else {
      /* this block is executed by the parent thread */

      // close write end of pipe in parent
      close(pipe_fds[1]);

      // wait until the transformer JVM has indicated that class transformations
      // can begin
      LOG_VERBOSE("Waiting for the transformer JVM to register itself.\n");
      int transformer_port =
          g_class_transformation_server->WaitForBeginClassTransformations(30);
      if (transformer_port == -1) {
        // there was a timeout
        std::cerr << "Transformer JVM failed to register within 30 seconds"
                  << std::endl;
        cleanup();

        // read exit code, if applicable
        int transformer_status;
        pid_t p = waitpid(transformer_pid, &transformer_status, WNOHANG);
        if (p == transformer_pid) {
          if (!WIFEXITED(transformer_status)) {
            // process may be still alive, kill it
            std::cerr << "Killing transformer JVM" << std::endl;
            if (kill(transformer_pid, 9) == 0) {
              waitpid(transformer_pid, NULL, 0);
            }
          } else {
            // process is dead, return its status
            std::cerr << "Transformer JVM exited with status: "
                      << WEXITSTATUS(transformer_status) << std::endl;
            return WEXITSTATUS(transformer_status);
          }
        } else if (p == 0) {
          // process is still running, interrupt it
          std::cerr << "Interrupting transformer JVM" << std::endl;
          if (kill(transformer_pid, 2) == 0) {
            waitpid(transformer_pid, NULL, 0);
          }
        } else if (p == -1) {
          // something else went wrong
          int errnum = errno;
          std::cerr << "Unknown error getting transformer JVM's status: "
                    << std::strerror(errnum) << ": " << errnum << std::endl;
          return errnum;
        }

        return JNI_ERR;
      } else {
        // set the transformer's address ourselves
        cli_options.transformer_address =
            std::string("0.0.0.0:" + std::to_string(transformer_port));
      }
      LOG_VERBOSE("Started transformer JVM on port '%d'.\n", transformer_port);
    }
  }
  LOG_VERBOSE("Using transformer JVM at '%s'.\n",
              cli_options.transformer_address.c_str());

  // build the client that talks to the transformer JVM
  LOG_VERBOSE("Creating client to talk to the transformer JVM.\n");
  g_class_transformation_client =
      new ClassTransformationClient(grpc::CreateChannel(
          cli_options.transformer_address, grpc::InsecureChannelCredentials()));

  LOG_VERBOSE("Agent loaded successfully.\n");
  return JNI_OK;
}

// called by the JVM to unload the agent
JNIEXPORT void JNICALL Agent_OnUnload(JavaVM *vm) { cleanup(); }

// function to be called for each class loaded by the JVM
static void JNICALL ClassFileLoadHookCallback(
    jvmtiEnv *jvmti_env, JNIEnv *jni_env, jclass class_being_redefined,
    jobject loader, const char *name, jobject protection_domain,
    jint class_data_len, const unsigned char *class_data,
    jint *new_class_data_len, unsigned char **new_class_data) {
  // keep track of which classes have been loaded already
  static bool java_io_FileInputStream_seen = false;
  static bool java_io_FileOutputStream_seen = false;
  static bool java_io_RandomAccessFile_seen = false;
  static bool java_lang_Shutdown_seen = false;
  static bool java_nio_DirectByteBuffer_seen = false;
  static bool java_nio_DirectByteBufferR_seen = false;
  static bool java_nio_MappedByteBuffer_seen = false;
  static bool java_util_zip_ZipFile_seen = false;
  static bool sun_nio_ch_FileChannelImpl_seen = false;

  // all transformations done
  if (java_io_FileInputStream_seen && java_io_FileOutputStream_seen &&
      java_io_RandomAccessFile_seen && java_nio_DirectByteBuffer_seen &&
      java_nio_DirectByteBufferR_seen && java_nio_MappedByteBuffer_seen &&
      java_lang_Shutdown_seen && java_util_zip_ZipFile_seen &&
      sun_nio_ch_FileChannelImpl_seen) {
    LOG_VERBOSE("Ignoring class '%s' because all required classes have been "
                "transformed.\n",
                name);
    return;
  }

  if (jvmti_env == NULL) {
    std::cerr << "Received class " << name
              << " during primordial phase, ignoring it" << std::endl;
    return;
  }

  // wrap JVMTI so the client does not need to know about memory allocation
  // details
  std::function<unsigned char *(int)> allocator = [jvmti_env](int size) {
    unsigned char *out = NULL;
    jvmtiError jvmti_result = jvmti_env->Allocate(size, &out);
    CHECK_JVMTI_RESULT_NORET("Allocate", jvmti_result);
    return out;
  };

  bool transform_class = true;
  if (strcmp(name, "java/io/FileInputStream") == 0) {
    java_io_FileInputStream_seen = true;
  } else if (strcmp(name, "java/io/FileOutputStream") == 0) {
    java_io_FileOutputStream_seen = true;
  } else if (strcmp(name, "java/io/RandomAccessFile") == 0) {
    java_io_RandomAccessFile_seen = true;
  } else if (strcmp(name, "java/lang/Shutdown") == 0) {
    java_lang_Shutdown_seen = true;
  } else if (strcmp(name, "java/nio/DirectByteBuffer") == 0) {
    java_nio_DirectByteBuffer_seen = true;
  } else if (strcmp(name, "java/nio/DirectByteBufferR") == 0) {
    java_nio_DirectByteBufferR_seen = true;
  } else if (strcmp(name, "java/nio/MappedByteBuffer") == 0) {
    java_nio_MappedByteBuffer_seen = true;
  } else if (strcmp(name, "java/util/zip/ZipFile") == 0) {
    java_util_zip_ZipFile_seen = true;
  } else if (strcmp(name, "sun/nio/ch/FileChannelImpl") == 0) {
    sun_nio_ch_FileChannelImpl_seen = true;
  } else {
    // don't set new_class_data_len or new_class_data to indicate no
    // modification is desired
    transform_class = false;
  }

  if (transform_class) {
    LOG_VERBOSE("Transforming class '%s'.\n", name);
    g_class_transformation_client->ClassTransformation(
        name, class_data, class_data_len, allocator, new_class_data,
        new_class_data_len, g_native_method_prefix.c_str());
    LOG_VERBOSE("Transformed class '%s'.\n", name);
  } else {
    LOG_VERBOSE("Not transforming class '%s'.\n", name);
  }

  // indicate after all necessary classes are loaded that the transformer
  // JVM can shut down, if we have started it ourselves
  if (java_io_FileInputStream_seen && java_io_FileOutputStream_seen &&
      java_io_RandomAccessFile_seen && java_lang_Shutdown_seen &&
      java_nio_DirectByteBuffer_seen && java_nio_DirectByteBufferR_seen &&
      java_nio_MappedByteBuffer_seen && java_util_zip_ZipFile_seen &&
      sun_nio_ch_FileChannelImpl_seen) {
    LOG_VERBOSE("All required classes have been transformed.\n");
    if (g_start_transformer_jvm) {
      LOG_VERBOSE("Stopping transformer JVM.\n");
      g_class_transformation_client->EndClassTransformations();
      LOG_VERBOSE("Stopped transformer JVM.\n");
    }
    cleanup();
  }
}

// function to be called when the JVM initializes
static void JNICALL VMInitCallback(jvmtiEnv *jvmti_env, JNIEnv *jni_env,
                                   jthread thread) {
  LOG_VERBOSE("Initializing VM.\n");

  // trigger loading (and thus instrumentation) of the classes if they have not
  // been loaded yet
  LOG_VERBOSE("Finding required classes.\n");
  jni_env->FindClass("java/io/FileInputStream");
  jni_env->FindClass("java/io/FileOutputStream");
  jni_env->FindClass("java/io/RandomAccessFile");
  jni_env->FindClass("java/lang/Shutdown");
  jni_env->FindClass("java/nio/DirectByteBuffer");
  jni_env->FindClass("java/nio/DirectByteBufferR");
  jni_env->FindClass("java/nio/MappedByteBuffer");
  jni_env->FindClass("java/util/zip/ZipFile");
  jni_env->FindClass("sun/nio/ch/FileChannelImpl");

  // set the hostname as system property
  jclass system_class = jni_env->FindClass("java/lang/System");

  jmethodID set_property_method_id = jni_env->GetStaticMethodID(
      system_class, "setProperty",
      "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");

  char hostname[256];
  if (gethostname(hostname, 256) != 0) {
    std::cerr << "Error getting hostname" << std::endl;
  } else {
    LOG_VERBOSE("Setting system property '%s'='%s'.\n",
                std::string("de.zib.sfs.hostname").c_str(), hostname);
    jni_env->CallStaticVoidMethod(system_class, set_property_method_id,
                                  jni_env->NewStringUTF("de.zib.sfs.hostname"),
                                  jni_env->NewStringUTF(hostname));
  }

  // repeat for the PID
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.pid").c_str(),
              std::to_string(getpid()).c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.pid"),
      jni_env->NewStringUTF(std::to_string(getpid()).c_str()));

  // repeat for the custom key
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.key").c_str(), g_key.c_str());
  jni_env->CallStaticVoidMethod(system_class, set_property_method_id,
                                jni_env->NewStringUTF("de.zib.sfs.key"),
                                jni_env->NewStringUTF(g_key.c_str()));

  // repeat for the time bin duration
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.timeBin.duration").c_str(),
              g_time_bin_duration.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.timeBin.duration"),
      jni_env->NewStringUTF(g_time_bin_duration.c_str()));

  // repeat for the time bin cache size
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.timeBin.cacheSize").c_str(),
              g_time_bin_cache_size.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.timeBin.cacheSize"),
      jni_env->NewStringUTF(g_time_bin_cache_size.c_str()));

  // repeat for the output directory
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.output.directory").c_str(),
              g_output_directory.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.output.directory"),
      jni_env->NewStringUTF(g_output_directory.c_str()));

  // repeat for the output format
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.output.format").c_str(),
              g_output_format.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.output.format"),
      jni_env->NewStringUTF(g_output_format.c_str()));

  // repeat for the operation statistics pool size
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.operationStatistics.poolSize").c_str(),
              g_os_pool_size.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.operationStatistics.poolSize"),
      jni_env->NewStringUTF(g_os_pool_size.c_str()));

  // repeat for the data operation statistics pool size
  LOG_VERBOSE(
      "Setting system property '%s'='%s'.\n",
      std::string("de.zib.sfs.dataOperationStatistics.poolSize").c_str(),
      g_dos_pool_size.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.dataOperationStatistics.poolSize"),
      jni_env->NewStringUTF(g_dos_pool_size.c_str()));

  // repeat for the read data operation statistics pool size
  LOG_VERBOSE(
      "Setting system property '%s'='%s'.\n",
      std::string("de.zib.sfs.readDataOperationStatistics.poolSize").c_str(),
      g_rdos_pool_size.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.readDataOperationStatistics.poolSize"),
      jni_env->NewStringUTF(g_rdos_pool_size.c_str()));

  // repeat for the task queue size
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.queueSize").c_str(),
              g_tq_pool_size.c_str());
  jni_env->CallStaticVoidMethod(system_class, set_property_method_id,
                                jni_env->NewStringUTF("de.zib.sfs.queueSize"),
                                jni_env->NewStringUTF(g_tq_pool_size.c_str()));

  // repeat for the mmap directory
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.mmap.directory").c_str(),
              g_mmap_directory.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.mmap.directory"),
      jni_env->NewStringUTF(g_mmap_directory.c_str()));

  // repeat for the LongQueue lock cache size
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.longQueue.lockCacheSize").c_str(),
              g_lq_lock_cache_size.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.longQueue.lockCacheSize"),
      jni_env->NewStringUTF(g_lq_lock_cache_size.c_str()));

  // repeat for the MemoryPool lock cache size
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.memoryPool.lockCacheSize").c_str(),
              g_mp_lock_cache_size.c_str());
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.memoryPool.lockCacheSize"),
      jni_env->NewStringUTF(g_mp_lock_cache_size.c_str()));

  // repeat for the tracing of mmap calls
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.traceMmap").c_str(),
              g_trace_mmap ? "true" : "false");
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.traceMmap"),
      jni_env->NewStringUTF(g_trace_mmap ? "true" : "false"));

  // repeat for the tracing of file descriptors
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.traceFds").c_str(),
              g_trace_fds ? "true" : "false");
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.traceFds"),
      jni_env->NewStringUTF(g_trace_fds ? "true" : "false"));

  // repeat for the verbosity
  LOG_VERBOSE("Setting system property '%s'='%s'.\n",
              std::string("de.zib.sfs.verbose").c_str(),
              g_verbose ? "true" : "false");
  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.verbose"),
      jni_env->NewStringUTF(g_verbose ? "true" : "false"));

  // tell the aggregator to initialize
  LOG_VERBOSE("Initializing OperationStatisticsAggregator.\n");
  jclass live_operation_statistics_aggregator_class = jni_env->FindClass(
      "de/zib/sfs/instrument/statistics/LiveOperationStatisticsAggregator");
  jfieldID instance_field_id = jni_env->GetStaticFieldID(
      live_operation_statistics_aggregator_class, "instance",
      "Lde/zib/sfs/instrument/statistics/LiveOperationStatisticsAggregator;");
  jobject live_operation_statistics_aggregator_instance =
      jni_env->GetStaticObjectField(live_operation_statistics_aggregator_class,
                                    instance_field_id);
  jmethodID initialize_method_id = jni_env->GetMethodID(
      live_operation_statistics_aggregator_class, "initialize", "()V");
  jni_env->CallVoidMethod(live_operation_statistics_aggregator_instance,
                          initialize_method_id);

  // print stack trace of exception during initialization, if any
  if (jni_env->ExceptionCheck() == JNI_TRUE) {
    LOG_VERBOSE("Exception during VM initialization.\n");

    // print stacktrace and rethrow because ExceptionDescribe clears the pending
    // exception
    jthrowable exception = jni_env->ExceptionOccurred();
    jni_env->ExceptionDescribe();
    jni_env->Throw(exception);
  } else {
    LOG_VERBOSE("VM initialized successfully.\n");
  }
}

// function to be call when the JVM shuts down
static void JNICALL VMDeathCallback(jvmtiEnv *jvmti_env, JNIEnv *jni_env) {
  LOG_VERBOSE("Shutting down VM.\n");

  // get the aggregator and shut it down
  jclass live_operation_statistics_aggregator_class = jni_env->FindClass(
      "de/zib/sfs/instrument/statistics/LiveOperationStatisticsAggregator");
  jfieldID instance_field_id = jni_env->GetStaticFieldID(
      live_operation_statistics_aggregator_class, "instance",
      "Lde/zib/sfs/instrument/statistics/LiveOperationStatisticsAggregator;");
  jobject live_operation_statistics_aggregator_instance =
      jni_env->GetStaticObjectField(live_operation_statistics_aggregator_class,
                                    instance_field_id);

  LOG_VERBOSE("Shutting down LiveOperationStatisticsAggregator.\n");
  jmethodID shutdown_method_id = jni_env->GetMethodID(
      live_operation_statistics_aggregator_class, "shutdown", "()V");
  jni_env->CallVoidMethod(live_operation_statistics_aggregator_instance,
                          shutdown_method_id);
  LOG_VERBOSE("LiveOperationStatisticsAggregator shut down successfully.\n");

  LOG_VERBOSE("VM shut down successfully.\n");
}

// performs deregistration of events, server shutdown and memory freeing
static void cleanup() {
  LOG_VERBOSE("Cleaning up.\n");

  // shut down the client that talked to the transformer JVM
  if (g_class_transformation_client != NULL) {
    LOG_VERBOSE("Deleting client.\n");
    delete g_class_transformation_client;
    g_class_transformation_client = NULL;
  }

  // shut down the server the transformer JVM talked to
  if (g_class_transformation_server != NULL) {
    LOG_VERBOSE("Shutting down transformation server.\n");
    g_class_transformation_server->Shutdown();
    delete g_class_transformation_server;
    g_class_transformation_server = NULL;
  }

  // clean the startup command for the transformer JVM
  if (g_transformer_jvm_cmd != NULL) {
    LOG_VERBOSE("Freeing transformer JVM command.\n");
    for (size_t i = 0; i < 12; ++i) {
      free(g_transformer_jvm_cmd[i]);
    }
    delete[] g_transformer_jvm_cmd;
    g_transformer_jvm_cmd = NULL;
  }

  LOG_VERBOSE("Cleaned up successfully.\n");
}
