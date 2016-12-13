/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
#ifndef ERROR_MACROS_H
#define ERROR_MACROS_H

#include <iostream>

// prints an error for a function within a component, if its return value does
// not indicate success
#define CHECK_RESULT(component, name, result, ok)                              \
  do {                                                                         \
    if ((result) != (ok)) {                                                    \
      std::cerr << (component) << ": " << (name) << " returned " << (result)   \
                << "." << std::endl;                                           \
      return (result);                                                         \
    }                                                                          \
  } while (false)
#define CHECK_RESULT_NORET(component, name, result, ok)                        \
  do {                                                                         \
    if ((result) != (ok)) {                                                    \
      std::cerr << (component) << ": " << (name) << " returned " << (result)   \
                << "." << std::endl;                                           \
    }                                                                          \
  } while (false)

// convenience specialization for JNI
#define CHECK_JNI_RESULT(name, result)                                         \
  CHECK_RESULT("JNI", (name), (result), JNI_OK)
#define CHECK_JNI_RESULT_NORET(name, result)                                   \
  CHECK_RESULT_NORET("JNI", (name), (result), JNI_OK)

// convenience specialization for JVMTI
#define CHECK_JVMTI_RESULT(name, result)                                       \
  CHECK_RESULT("JVMTI", (name), (result), JVMTI_ERROR_NONE)
#define CHECK_JVMTI_RESULT_NORET(name, result)                                 \
  CHECK_RESULT_NORET("JVMTI", (name), (result), JVMTI_ERROR_NONE)

#endif // ERROR_MACROSL_H
