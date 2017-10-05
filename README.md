# hdfs-statistics-adapter
Wrapper around HDFS and core Java I/O classes for collecting file statistics.

It consists of two parts, [sfs-agent](sfs-agent) which takes care of instrumenting `java.io.FileInputStream`, `java.io.FileOutputStream`, `java.io.RandomAccessFile` and `sun.nio.ch.FileChannelImpl`, and [sfs-adapter](sfs-adapter) which can be used as a drop-in wrapper around various other HDFS compatible file systems, as found in Hadoop and Flink.

For details on how to use with Hadoop and Flink, see [.travis.yml](.travis.yml), [scripts/travis/hadoop_grep.sh](scripts/travis/hadoop_grep.sh), [scripts/slurm/start-hdfs-slurm.sh](scripts/slurm/start-hdfs-slurm.sh) and [scripts/slurm/terasort-hdfs.sh](scripts/slurm/terasort-hdfs.sh)

## Building
Please also check [.travis.yml](.travis.yml) for how to obtain and build the appropriate [gRPC](https://grpc.io/), [protobuf](https://developers.google.com/protocol-buffers/) and [FlatBuffers](https://google.github.io/flatbuffers/) versions.

```bash
PROTOBUF_VERSION=3.4.0
GRPC_VERSION=1.6.1
GRPC_HOME=/path/to/grpc
FLATBUFFERS_VERSION=1.7.1-1-SNAPSHOT
FLATC_BIN=/path/to/flatc
TMPDIR=/tmp

export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$GRPC_HOME/libs/opt:$GRPC_HOME/third_party/protobuf/src/.libs"

mvn clean install --define protobuf.version=$PROTOBUF_VERSION \
  --define flatc.bin=$FLATC_BIN \
  --define flatbuffers.version=$FLATBUFFERS_VERSION \
  --define protoc.bin=$GRPC_HOME/third_party/protobuf/src/protoc \
  --define protobuf.include.dir=$GRPC_HOME/third_party/protobuf/src \
  --define protobuf.lib.dir=$GRPC_HOME/third_party/protobuf/src/.libs \
  --define grpc.version=$GRPC_VERSION \
  --define grpc.plugin=$GRPC_HOME/bins/opt/grpc_cpp_plugin \
  --define grpc.include.dir=$GRPC_HOME/include \
  --define grpc.lib.dir=$GRPC_HOME/libs/opt \
  --define java.io.tmpdir=$TMPDIR
```

[![Build Status](https://travis-ci.org/robert-schmidtke/hdfs-statistics-adapter.svg?branch=master)](https://travis-ci.org/robert-schmidtke/hdfs-statistics-adapter)
