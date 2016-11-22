HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += -I/usr/local/include -pthread -I${JDK_HOME}/include -Isrc/main/native/agent -Itarget/generated-sources/protobuf/native -c -fPIC -fpermissive
CXXFLAGS += -std=c++11
ifeq ($(SYSTEM),Darwin)
CPPFLAGS += -I${JDK_HOME}/include/darwin
LDFLAGS += -L/usr/local/lib `pkg-config --libs grpc++ grpc`       \
           -lgrpc++_reflection                                    \
           -lprotobuf -lpthread -ldl                              \
           -dynamiclib -o target/libsfs.dylib
else
CPPFLAGS += -I${JDK_HOME}/include/linux
LDFLAGS += -L/usr/local/lib `pkg-config --libs grpc++ grpc`       \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed \
           -lprotobuf -lpthread -ldl                              \
           -z defs -static-libgcc -shared -o target/libsfs.so -lc
endif

sfs: sfs_pb sfs_grpc_pb
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o target/sfs.o -c src/main/native/agent/sfs.cpp
	$(CXX) $(LDFLAGS) target/sfs_grpc_pb.o target/sfs_pb.o target/sfs.o

sfs_pb:
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o target/sfs_pb.o -c target/generated-sources/protobuf/native/agent/rpc/proto/sfs.pb.cc

sfs_grpc_pb:
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o target/sfs_grpc_pb.o -c target/generated-sources/protobuf/native/agent/rpc/proto/sfs.grpc.pb.cc
	
clean:
	rm -rf target/sfs_pb.o target/sfs_grpc_pb.o target/sfs.o target/libsfs.so target/libsfs.dylib