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

#include "agent/rpc/sfs.grpc.pb.h"

using grpc::ChannelArguments;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerBuilderOption;
using grpc::ServerBuilderPlugin;
using grpc::ServerContext;
using grpc::Status;

using de::zib::sfs::instrument::rpc::BeginClassTransformationsRequest;
using de::zib::sfs::instrument::rpc::BeginClassTransformationsResponse;
using de::zib::sfs::instrument::rpc::ClassTransformationRequest;
using de::zib::sfs::instrument::rpc::ClassTransformationResponse;
using de::zib::sfs::instrument::rpc::ClassTransformationService;
using de::zib::sfs::instrument::rpc::EndClassTransformationsRequest;
using de::zib::sfs::instrument::rpc::EndClassTransformationsResponse;

class ClassTransformationServerOptions : public ServerBuilderOption {
public:
  void UpdateArguments(ChannelArguments *args) override {
    // explicitly disallow reusing ports, this is for Mac OS X mainly, Linux
    // does not allow this by default
    args->SetInt(GRPC_ARG_ALLOW_REUSEPORT, 0);
  }

  void
  UpdatePlugins(std::vector<std::unique_ptr<ServerBuilderPlugin>> *) override {}
};

class ClassTransformationServer : public ClassTransformationService::Service {
private:
  std::unique_ptr<Server> server_;

  int transformer_port_;
  std::mutex begin_class_transformations_mutex_;
  std::condition_variable begin_class_transformations_cond_;

public:
  ClassTransformationServer() { transformer_port_ = -1; }

  Status
  BeginClassTransformations(ServerContext *context,
                            const BeginClassTransformationsRequest *request,
                            BeginClassTransformationsResponse *response) {
    {
      std::lock_guard<std::mutex> lock(begin_class_transformations_mutex_);
      transformer_port_ = request->port();
    }
    begin_class_transformations_cond_.notify_one();
    return Status::OK;
  }

  bool Start(std::string address) {
    if (server_ == nullptr) {
      ServerBuilder server_builder;
      server_builder.AddListeningPort(address,
                                      grpc::InsecureServerCredentials());
      server_builder.SetOption(std::unique_ptr<ServerBuilderOption>(
          new ClassTransformationServerOptions));
      server_builder.RegisterService(this);
      server_ = server_builder.BuildAndStart();
    }
    return server_ != nullptr;
  }

  void Shutdown() {
    if (server_ != nullptr) {
      server_->Shutdown();
      server_->Wait();
      server_ = nullptr;
    }
  }

  int WaitForBeginClassTransformations(int timeout_seconds) {
    {
      std::unique_lock<std::mutex> lock(begin_class_transformations_mutex_);
      begin_class_transformations_cond_.wait_for(
          lock, std::chrono::seconds(timeout_seconds),
          [this] { return this->transformer_port_ != -1; });
    }
    return transformer_port_;
  }
};

#endif // CLASS_TRANSFORMATION_SERVER_H
