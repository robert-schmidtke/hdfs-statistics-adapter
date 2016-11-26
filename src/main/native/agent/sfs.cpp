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

// store startup command of transformer JVM so we can them up
static char **g_transformer_jvm_cmd;

// the prefix to use when wrapping native methods
static std::string g_native_method_prefix("sfs_native_");

// the name of the log file to use
static std::string g_log_file_name;

// performs deregistration of events, server shutdown and memory freeing
static void cleanup();

// function to be called for each class loaded by the JVM
static void JNICALL ClassFileLoadHookCallback(jvmtiEnv *, JNIEnv *, jclass,
                                              jobject, const char *, jobject,
                                              jint, const unsigned char *,
                                              jint *, unsigned char **);

// function to be called when the JVM initializes
static void JNICALL VMInitCallback(jvmtiEnv *, JNIEnv *, jthread);

// called by the JVM to load the agent
JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *vm, char *options, void *reserved) {
  // parse command line options
  CliOptions cli_options;
  if (!parse_options(options, &cli_options)) {
    std::cerr << "Could not parse options: "
              << (options != NULL ? options : "-") << std::endl
              << "Required options: " << std::endl
              << "  trans_jar=/path/to/trans.jar" << std::endl
              << "  comm_port_agent=port" << std::endl
              << "  comm_port_trans=port" << std::endl
              << "  log_file_name=/path/to/log.file" << std::endl;
    return JNI_EINVAL;
  }

  // make sure a valid JAVA_HOME is set
  if (getenv("JAVA_HOME") == NULL) {
    std::cerr << "JAVA_HOME not set" << std::endl;
    return JNI_EINVAL;
  }

  // we append the slash later on
  std::string java_home(getenv("JAVA_HOME"));
  if (java_home.back() == '/') {
    java_home.pop_back();
  }

  // variables used for error checking
  jint jni_result = JNI_OK;
  jvmtiError jvmti_result = JVMTI_ERROR_NONE;

  // obtain the JVMTI environment
  jvmtiEnv *jvmti = NULL;
  jni_result = vm->GetEnv((void **)&jvmti, JVMTI_VERSION);
  CHECK_JNI_RESULT("GetEnv", jni_result);

  // register required capabilities:
  // - get notified upon class definitions
  // - allow rewriting their bytecodes and signatures
  jvmtiCapabilities capabilities;
  (void)memset(&capabilities, 0, sizeof(jvmtiCapabilities));
  capabilities.can_generate_all_class_hook_events = 1;
  capabilities.can_retransform_any_class = 1;
  capabilities.can_retransform_classes = 1;
  capabilities.can_set_native_method_prefix = 1;
  jvmti_result = jvmti->AddCapabilities(&capabilities);
  CHECK_JVMTI_RESULT("AddCapabilities", jvmti_result);

  // register callback to be called when classes are loaded
  jvmtiEventCallbacks eventCallbacks;
  (void)memset(&eventCallbacks, 0, sizeof(eventCallbacks));
  eventCallbacks.ClassFileLoadHook = &ClassFileLoadHookCallback;
  eventCallbacks.VMInit = &VMInitCallback;
  jvmti_result =
      jvmti->SetEventCallbacks(&eventCallbacks, (jint)sizeof(eventCallbacks));
  CHECK_JVMTI_RESULT("SetEventCallbacks", jvmti_result);

  // enable notification sending on class loading
  jvmti_result = jvmti->SetEventNotificationMode(
      JVMTI_ENABLE, JVMTI_EVENT_CLASS_FILE_LOAD_HOOK, (jthread)NULL);
  CHECK_JVMTI_RESULT("SetEventNotificationMode(ClassFileLoadHook)",
                     jvmti_result);
  jvmti_result = jvmti->SetEventNotificationMode(
      JVMTI_ENABLE, JVMTI_EVENT_VM_INIT, (jthread)NULL);
  CHECK_JVMTI_RESULT("SetEventNotificationMode(VMInit)", jvmti_result);

  // create a log file name for this JVM
  g_log_file_name = cli_options.log_file_name + "." + std::to_string(getpid());

  // set the prefix to use when wrapping native methods
  jvmti_result = jvmti->SetNativeMethodPrefix(g_native_method_prefix.c_str());
  CHECK_JVMTI_RESULT("SetNativeMethodPrefix", jvmti_result);

  // build the transformer JVM start command
  g_transformer_jvm_cmd = new char *[9];
  g_transformer_jvm_cmd[0] = strdup((java_home + "/bin/java").c_str());
  g_transformer_jvm_cmd[1] = strdup("-cp");
  g_transformer_jvm_cmd[2] = strdup(cli_options.transformer_jar_path.c_str());
  g_transformer_jvm_cmd[3] =
      strdup("de.zib.sfs.instrument.ClassTransformationService");
  g_transformer_jvm_cmd[4] = strdup("--communication-port-agent");
  g_transformer_jvm_cmd[5] =
      strdup(std::to_string(cli_options.communication_port_agent).c_str());
  g_transformer_jvm_cmd[6] = strdup("--communication-port-transformer");
  g_transformer_jvm_cmd[7] = strdup(
      std::to_string(cli_options.communication_port_transformer).c_str());
  g_transformer_jvm_cmd[8] = NULL;

  char *envp[] = {NULL};

  // start the server that communicates with the transformer JVM
  g_class_transformation_server = new ClassTransformationServer;
  g_class_transformation_server->Start(
      "0.0.0.0:" + std::to_string(cli_options.communication_port_agent));

  // start new process to execute the transformer JVM
  pid_t transformer_pid = fork();
  if (transformer_pid == 0) {
    /* this block is executed by the child thread */

    // start the transformer JVM, execve does not return on success
    execve((java_home + "/bin/java").c_str(), g_transformer_jvm_cmd, envp);
    int errnum = errno;
    std::cerr << "Could not start transformer JVM (" << std::strerror(errnum)
              << ": " << errnum << ")" << std::endl;
    return errnum;
  } else {
    /* this block is executed by the parent thread */

    // wait until the transformer JVM has indicated that class transformations
    // can begin
    if (!g_class_transformation_server->WaitForBeginClassTransformations(30)) {
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
      // build the client that talks to the transformer JVM
      g_class_transformation_client =
          new ClassTransformationClient(grpc::CreateChannel(
              "0.0.0.0:" +
                  std::to_string(cli_options.communication_port_transformer),
              grpc::InsecureChannelCredentials()));
      return JNI_OK;
    }
  }
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
  static bool sun_nio_ch_FileChannelImpl_seen = false;

  // all transformations done
  if (java_io_FileInputStream_seen && java_io_FileOutputStream_seen &&
      sun_nio_ch_FileChannelImpl_seen) {
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

  if (strcmp(name, "java/io/FileInputStream") == 0) {
    java_io_FileInputStream_seen = true;
    g_class_transformation_client->ClassTransformation(
        name, class_data, class_data_len, allocator, new_class_data,
        new_class_data_len, g_native_method_prefix.c_str(),
        g_log_file_name.c_str());
  } else if (strcmp(name, "java/io/FileOutputStream") == 0) {
    java_io_FileOutputStream_seen = true;
    g_class_transformation_client->ClassTransformation(
        name, class_data, class_data_len, allocator, new_class_data,
        new_class_data_len, g_native_method_prefix.c_str(),
        g_log_file_name.c_str());
  } else if (strcmp(name, "sun/nio/ch/FileChannelImpl") == 0) {
    sun_nio_ch_FileChannelImpl_seen = true;
    g_class_transformation_client->ClassTransformation(
        name, class_data, class_data_len, allocator, new_class_data,
        new_class_data_len, g_native_method_prefix.c_str(),
        g_log_file_name.c_str());
  } else {
    // don't set new_class_data_len or new_class_data to indicate no
    // modification is desired
  }

  // indicate after all necessary classes are loaded that the transformer
  // JVM can shut down
  if (java_io_FileInputStream_seen && java_io_FileOutputStream_seen &&
      sun_nio_ch_FileChannelImpl_seen) {
    g_class_transformation_client->EndClassTransformations();
    cleanup();
  }
}

// function to be called when the JVM initializes
static void JNICALL VMInitCallback(jvmtiEnv *jvmti_env, JNIEnv *jni_env,
                                   jthread thread) {
  // trigger loading (and thus instrumentation) of the classes if they have not
  // been loaded yet
  jni_env->FindClass("java/io/FileInputStream");
  jni_env->FindClass("java/io/FileOutputStream");
  jni_env->FindClass("sun/nio/ch/FileChannelImpl");

  // set the log file name to use as system property
  jclass system_class = jni_env->FindClass("java/lang/System");

  jmethodID set_property_method_id = jni_env->GetStaticMethodID(
      system_class, "setProperty",
      "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");

  jni_env->CallStaticVoidMethod(
      system_class, set_property_method_id,
      jni_env->NewStringUTF("de.zib.sfs.asyncLogFileName"),
      jni_env->NewStringUTF(g_log_file_name.c_str()));

  // TODO set de.zib.sfs.hostname as well
  // TODO mkdir the parent log file directories
}

// performs deregistration of events, server shutdown and memory freeing
static void cleanup() {
  // shut down the client that talked to the transformer JVM
  if (g_class_transformation_client != NULL) {
    delete g_class_transformation_client;
    g_class_transformation_client = NULL;
  }

  // shut down the server the transformer JVM talked to
  if (g_class_transformation_server != NULL) {
    g_class_transformation_server->Shutdown();
    delete g_class_transformation_server;
    g_class_transformation_server = NULL;
  }

  // clean the startup command for the transformer JVM
  if (g_transformer_jvm_cmd != NULL) {
    for (size_t i = 0; i < 8; ++i) {
      free(g_transformer_jvm_cmd[i]);
    }
    delete[] g_transformer_jvm_cmd;
    g_transformer_jvm_cmd = NULL;
  }
}
