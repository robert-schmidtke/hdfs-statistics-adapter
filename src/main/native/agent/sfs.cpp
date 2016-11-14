#include <jvmti.h>

#include <cstdio>
#include <cstring>

jvmtiEnv *g_jvmti = NULL;

static void JNICALL
ClassFileLoadHookCallback(
    jvmtiEnv *jvmti_env,
    JNIEnv *jni_env,
    jclass class_being_redefined,
    jobject loader,
    const char *name,
    jobject protection_domain,
    jint class_data_len,
    const unsigned char *class_data,
    jint *new_class_data_len,
    unsigned char **new_class_data) {
  // TODO listen on name=java/io/FileInputStream etc
}

JNIEXPORT jint JNICALL
Agent_OnLoad(JavaVM *vm, char *options, void *reserved) {
  vm->GetEnv((void **) &g_jvmti, JVMTI_VERSION);

  jvmtiCapabilities capabilities;
  (void) memset(&capabilities, 0, sizeof(jvmtiCapabilities));
  capabilities.can_generate_all_class_hook_events = 1;
  capabilities.can_retransform_classes = 1;
  g_jvmti->AddCapabilities(&capabilities);

  jvmtiEventCallbacks eventCallbacks;
  (void) memset(&eventCallbacks, 0, sizeof(eventCallbacks));
  eventCallbacks.ClassFileLoadHook = &ClassFileLoadHookCallback;
  g_jvmti->SetEventCallbacks(&eventCallbacks, (jint) sizeof(eventCallbacks));

  g_jvmti->SetEventNotificationMode(JVMTI_ENABLE, JVMTI_EVENT_CLASS_FILE_LOAD_HOOK,
      (jthread) NULL);

  return JNI_OK;
}

JNIEXPORT void JNICALL
Agent_OnUnload(JavaVM *vm) {
}
