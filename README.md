# hdfs-statistics-adapter
Wrapper around HDFS and core Java I/O classes for collecting file statistics.

It consists of two parts, [sfs-agent](sfs-agent) which takes care of instrumenting `java.io.FileInputStream`, `java.io.FileOutputStream`, `java.io.RandomAccessFile` and `sun.nio.ch.FileChannelImpl`, and [sfs-adapter](sfs-adapter) which can be used as a drop-in wrapper around various other HDFS compatible file systems, as found in Hadoop and Flink.

For details on building, please see [.travis.yml](.travis.yml). For details on how to use with Hadoop and Flink, see [.travis.yml](.travis.yml), [scripts/travis/hadoop_grep.sh](scripts/travis/hadoop_grep.sh), [scripts/slurm/start-hdfs-slurm.sh](scripts/slurm/start-hdfs-slurm.sh) and [scripts/slurm/terasort-hdfs.sh](scripts/slurm/terasort-hdfs.sh)

[![Build Status](https://travis-ci.org/robert-schmidtke/hdfs-statistics-adapter.svg?branch=master)](https://travis-ci.org/robert-schmidtke/hdfs-statistics-adapter)
