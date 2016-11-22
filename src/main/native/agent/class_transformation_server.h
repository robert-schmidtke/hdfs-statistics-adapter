/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
#ifndef CLASS_TRANSFORMATION_SERVER_H
#define CLASS_TRANSFORMATION_SERVER_H

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

#include <grpc++/grpc++.h>

#include "agent/rpc/proto/sfs.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using de::zib::sfs::agent::rpc::proto::BeginClassTransformationsRequest;
using de::zib::sfs::agent::rpc::proto::BeginClassTransformationsResponse;
using de::zib::sfs::agent::rpc::proto::ClassTransformationRequest;
using de::zib::sfs::agent::rpc::proto::ClassTransformationResponse;
using de::zib::sfs::agent::rpc::proto::ClassTransformationService;
using de::zib::sfs::agent::rpc::proto::EndClassTransformationsRequest;
using de::zib::sfs::agent::rpc::proto::EndClassTransformationsResponse;

class ClassTransformationServer : public ClassTransformationService::Service {
private:
  std::unique_ptr<Server> server_;

  bool is_begin_class_transformations_;
  std::mutex begin_class_transformations_mutex_;
  std::condition_variable begin_class_transformations_cond_;

public:
  ClassTransformationServer() { is_begin_class_transformations_ = false; }

  Status
  BeginClassTransformations(ServerContext *context,
                            const BeginClassTransformationsRequest *request,
                            BeginClassTransformationsResponse *response) {
    {
      std::lock_guard<std::mutex> lock(begin_class_transformations_mutex_);
      is_begin_class_transformations_ = true;
    }
    begin_class_transformations_cond_.notify_one();
    return Status::OK;
  }

  void Start(std::string address) {
    if (server_ == NULL) {
      ServerBuilder server_builder;
      server_builder.AddListeningPort(address,
                                      grpc::InsecureServerCredentials());
      server_builder.RegisterService(this);
      server_ = server_builder.BuildAndStart();
    }
  }

  void Shutdown() {
    if (server_ != NULL) {
      server_->Shutdown();
      server_->Wait();
      server_ = NULL;
    }
  }

  bool WaitForBeginClassTransformations(int timeout_seconds) {
    {
      std::unique_lock<std::mutex> lock(begin_class_transformations_mutex_);
      begin_class_transformations_cond_.wait_for(
          lock, std::chrono::seconds(timeout_seconds),
          [this] { return this->is_begin_class_transformations_; });
    }
    return is_begin_class_transformations_;
  }
};

#endif // CLASS_TRANSFORMATION_SERVER_H
