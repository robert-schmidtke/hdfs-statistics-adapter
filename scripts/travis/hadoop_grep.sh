#!/bin/bash

# some cleanup from possible previous runs
rm -rf /tmp/hadoop*
rm -rf /tmp/user/$USER

# setup Hadoop and install the adapter
echo localhost > $HADOOP_HOME/etc/hadoop/slaves
cp sfs-adapter/src/test/resources/hadoop/core-site.xml $HADOOP_HOME/etc/hadoop
cp sfs-adapter/src/test/resources/hadoop/hdfs-site.xml $HADOOP_HOME/etc/hadoop
cp sfs-adapter/src/test/resources/hadoop/mapred-site.xml $HADOOP_HOME/etc/hadoop
cp sfs-adapter/target/sfs-adapter.jar $HADOOP_HOME/share/hadoop/common

# make sure we wrap the local file system
line=$(grep -n "^    <name>sfs\.wrappedFS\.className<\/name>$" $HADOOP_HOME/etc/hadoop/core-site.xml | cut -d: -f1)
line=$(($line + 1))
sed -i "${line}s/.*/    <value>org.apache.hadoop.fs.RawLocalFileSystem<\/value>/" $HADOOP_HOME/etc/hadoop/core-site.xml
line=$(grep -n "^    <name>sfs\.wrappedFS\.scheme<\/name>$" $HADOOP_HOME/etc/hadoop/core-site.xml | cut -d: -f1)
line=$(($line + 1))
sed -i "${line}s/.*/    <value>file<\/value>/" $HADOOP_HOME/etc/hadoop/core-site.xml

export LD_LIBRARY_PATH_EXT="$GRPC_HOME/libs/opt:$GRPC_HOME/third_party/protobuf/src/.lib"

OPTS="-agentpath:$TRAVIS_BUILD_DIR/sfs-agent/target/libsfs.so=trans_jar=$TRAVIS_BUILD_DIR/sfs-agent/target/sfs-agent.jar,trans_address=0.0.0.0:4242"
export HADOOP_OPTS="$OPTS,log_file_name=/tmp/sfs.log.hadoop"
export YARN_OPTS="$OPTS,log_file_name=/tmp/sfs.log.yarn"
export MAP_OPTS="$OPTS,log_file_name=/tmp/sfs.log.map"
export REDUCE_OPTS="$OPTS,log_file_name=/tmp/sfs.log.reduce"

# instrument mappers and reducers
line_number=`grep -nr "</configuration>" "$HADOOP_HOME/etc/hadoop/mapred-site.xml" | cut -d : -f 1`
printf '%s\n' "${line_number}s#.*##" w | ed -s "$HADOOP_HOME/etc/hadoop/mapred-site.xml"
cat >> $HADOOP_HOME/etc/hadoop/mapred-site.xml << EOF
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>$MAP_OPTS</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>LD_LIBRARY_PATH=\${LD_LIBRARY_PATH}:${LD_LIBRARY_PATH_EXT}</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>$REDUCE_OPTS</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>LD_LIBRARY_PATH=\${LD_LIBRARY_PATH}:${LD_LIBRARY_PATH_EXT}</value>
  </property>
</configuration>
EOF

# start the transformer JVM
java -cp $TRAVIS_BUILD_DIR/sfs-agent/target/sfs-agent.jar de.zib.sfs.instrument.ClassTransformationService --port 4242 --timeout -1 2>&1 &
TRANSFORMER_PID=$!

# start Hadoop
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh

# run sample grep job
$HADOOP_HOME/bin/hdfs dfs -mkdir -p sfs:///tmp/user/$USER
$HADOOP_HOME/bin/hdfs dfs -put $HADOOP_HOME/etc/hadoop sfs:///tmp/user/$USER/input
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-$HADOOP_VERSION.jar grep sfs:///tmp/user/$USER/input sfs:///tmp/user/$USER/output 'dfs[a-z.]+'

echo "Hadoop Output:"
$HADOOP_HOME/bin/hdfs dfs -cat sfs:///tmp/user/$USER/output/*

echo "SFS Output:"
for file in $(ls /tmp/sfs.log*); do
  echo "${file}:"
  cat $file
done

# stop Hadoop
$HADOOP_HOME/sbin/stop-dfs.sh

# stop transformer JVM
kill $TRANSFORMER_PID
