#!/bin/bash

# some cleanup from possible previous runs
rm -rf /tmp/hadoop*
rm -rf /tmp/user/$USER

# setup Hadoop and install the adapter
cp src/test/resources/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop
cp src/test/resources/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop
cp src/test/resources/hadoop/mapred-site.xml $HADOOP_HOME/etc/hadoop
cp target/hdfs-statistics-adapter-1.0-SNAPSHOT.jar $HADOOP_HOME/share/hadoop/common

# make sure we wrap the local file system
line=$(grep -n "^    <name>sfs\.wrappedFS\.className<\/name>$" $HADOOP_HOME/etc/hadoop/core-site.xml | cut -d: -f1)
line=$(($line + 1))
sed -i "${line}s/.*/    <value>org.apache.hadoop.fs.RawLocalFileSystem<\/value>/" $HADOOP_HOME/etc/hadoop/core-site.xml

# start Hadoop
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

# run sample grep job
$HADOOP_HOME/bin/hdfs dfs -mkdir -p sfs:///tmp/user/$USER
$HADOOP_HOME/bin/hdfs dfs -put $HADOOP_HOME/etc/hadoop sfs:///tmp/user/$USER/input
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar grep sfs:///tmp/user/$USER/input sfs:///tmp/user/$USER/output 'dfs[a-z.]+'
$HADOOP_HOME/bin/hdfs dfs -cat sfs:///tmp/user/$USER/output/*

# stop Hadoop
$HADOOP_HOME/sbin/stop-dfs.sh
