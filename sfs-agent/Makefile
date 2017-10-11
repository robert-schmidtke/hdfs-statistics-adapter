HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += -I/usr/local/include -I${GRPC_INCLUDE_DIR} -I${JDK_HOME}/include -I${PROTOBUF_INCLUDE_DIR} \
            -Isrc/main/native/agent -Itarget/generated-sources/protobuf/native                         \
            -pthread -c -fPIC -fpermissive -Wall -O3
CXXFLAGS += -std=c++11
ifeq ($(SYSTEM),Darwin)
CPPFLAGS += -I${JDK_HOME}/include/darwin
LDFLAGS += -L/usr/local/lib -L${PROTOBUF_LIB_DIR} -L${GRPC_LIB_DIR} \
           -lgrpc++ -lgrpc -lgrpc++_reflection                      \
           -lprotobuf -lpthread -ldl                                \
           -dynamiclib -o target/libsfs.dylib
else
CPPFLAGS += -I${JDK_HOME}/include/linux
LDFLAGS += -L/usr/local/lib -L${PROTOBUF_LIB_DIR} -L${GRPC_LIB_DIR} \
           -lgrpc++ -lgrpc                                          \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed   \
           -lprotobuf -lpthread -ldl                                \
           -z defs -static-libgcc -shared -o target/libsfs.so -lc
endif

sfs: sfs_pb sfs_grpc_pb
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o target/sfs.o -c src/main/native/agent/sfs.cpp
	$(CXX) target/sfs_pb.o target/sfs_grpc_pb.o target/sfs.o $(LDFLAGS)

sfs_pb:
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o target/sfs_pb.o -c target/generated-sources/protobuf/native/agent/rpc/sfs.pb.cc

sfs_grpc_pb:
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o target/sfs_grpc_pb.o -c target/generated-sources/protobuf/native/agent/rpc/sfs.grpc.pb.cc

clean:
	rm -rf target/sfs_pb.o target/sfs_grpc_pb.o target/sfs.o target/libsfs.so target/libsfs.dylib