/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
#ifndef CLASS_TRANSFORMATION_CLIENT_H
#define CLASS_TRANSFORMATION_CLIENT_H

#include <cstring>
#include <functional>
#include <memory>

#include <grpc++/grpc++.h>

#include "agent/rpc/proto/sfs.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using de::zib::sfs::agent::rpc::proto::ClassTransformationRequest;
using de::zib::sfs::agent::rpc::proto::ClassTransformationResponse;
using de::zib::sfs::agent::rpc::proto::ClassTransformationService;
using de::zib::sfs::agent::rpc::proto::EndClassTransformationsRequest;
using de::zib::sfs::agent::rpc::proto::EndClassTransformationsResponse;

class ClassTransformationClient {
public:
  ClassTransformationClient(std::shared_ptr<Channel> channel)
      : stub_(ClassTransformationService::NewStub(channel)) {}

  void ClassTransformation(const char *name, const unsigned char *class_data,
                           int class_data_len,
                           std::function<unsigned char *(int)> allocator,
                           unsigned char **new_class_data,
                           int *new_class_data_len,
                           const char *native_method_prefix,
                           const char *ignore_file_name) {
    ClassTransformationRequest request;
    request.set_name(name);
    request.set_bytecode(std::string(reinterpret_cast<const char *>(class_data),
                                     class_data_len));
    request.set_native_method_prefix(native_method_prefix);
    request.set_ignore_file_name(ignore_file_name);
    ClassTransformationResponse response;
    ClientContext context;
    Status status = stub_->ClassTransformation(&context, request, &response);
    if (status.ok()) {
      *new_class_data = allocator(response.bytecode().size());
      if (*new_class_data != NULL) {
        std::memcpy(*new_class_data, reinterpret_cast<const unsigned char *>(
                                         response.bytecode().data()),
                    response.bytecode().size());
        *new_class_data_len = response.bytecode().size();
      }
    } else {
      // TODO
    }
  }

  void EndClassTransformations() {
    EndClassTransformationsRequest request;
    EndClassTransformationsResponse response;
    ClientContext context;
    Status status =
        stub_->EndClassTransformations(&context, request, &response);
    if (status.ok()) {

    } else {
      // TODO
    }
  }

private:
  std::unique_ptr<ClassTransformationService::Stub> stub_;
};

#endif // CLASS_TRANSFORMATION_CLIENT_H
