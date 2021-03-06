#!/bin/bash

usage() {
  echo "Usage: srun --nodes=1-1 --nodelist<NAMENODE> start-hdfs-slurm.sh"
  echo "  -b|--blocksize <block size in bytes> (default: 134217728)"
  echo "  -r|--replication <replication factor> (default: 1)"
  echo "  -m|--memory <total memory to be used on each node in megabytes> (default: 61440)"
  echo "  -o|--cores <total number of cores to be used on each node> (default: 16)"
  echo "  -i|--io-buffer <size of the IO buffer in bytes> (default: 1048576)"
  echo "  -h|--hadoop-opts the HADOOP_OPTS to set (default: not specified)"
  echo "  -a|--map-opts the JVM Options to pass to each mapper (default: not specified)"
  echo "  -e|--reduce-opts the JVM Options to pass to each reducer (default: not specified)"
  echo "  -y|--yarn-opts the YARN_OPTS to set (default: not specified)"
  echo "  -p|--mrappmaster-opts the JVM Options to pass to each MRAppMaster (default: not specified)"
  echo "  -l|--ld-library-path the LD_LIBRARY_PATH to set (default: not specified)"
  echo "  -c|--colocate-datanode-with-namenode (default: not specified/false)"
  echo "SFS specific options (default: not specified/do not use SFS):"
  echo "     --sfs-wrapped-fs <wrapped file system class name> (default: not specified; enables SFS if specified)"
  echo "     --sfs-wrapped-scheme <scheme of the wrapped file system> (default: not specified)"
  echo "     --sfs-instrumentation-skip <r|w|o> or any combination of them (default: not specified)"
  echo "     --sfs-output-directory </path/to/dir> (default: not specified)"
  echo "     --sfs-output-format csv|fb|bb (default: not specified)"
  echo "     --sfs-trace-fds true|false (default: not specified)"
}

echo "$(date): start-hdfs-slurm.sh $@"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected."
  usage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -b|--blocksize)
      BLOCKSIZE="$2"
      shift
      ;;
    -r|--replication)
      REPLICATION="$2"
      shift
      ;;
    -m|--memory)
      MEMORY="$2"
      shift
      ;;
    -o|--cores)
      CORES="$2"
      shift
      ;;
    -i|--io-buffer)
      IO_BUFFER="$2"
      shift
      ;;
    -c|--colocate-datanode-with-namenode)
      COLOCATE_DATANODE_WITH_NAMENODE="true"
      ;;
    -h|--hadoop-opts)
      HADOOP_OPTS="$2"
      shift
      ;;
    -a|--map-opts)
      MAP_OPTS="$2"
      shift
      ;;
    -e|--reduce-opts)
      REDUCE_OPTS="$2"
      shift
      ;;
    -y|--yarn-opts)
      YARN_OPTS="$2"
      shift
      ;;
    -p|--mrappmaster-opts)
      MRAPPMASTER_OPTS="$2"
      shift
      ;;
    -l|--ld-library-path)
      LD_LIBRARY_PATH_EXT="$2"
      shift
      ;;
    --sfs-wrapped-fs)
      SFS_WRAPPED_FS="$2"
      shift
      ;;
    --sfs-wrapped-scheme)
      SFS_WRAPPED_SCHEME="$2"
      shift
      ;;
    --sfs-instrumentation-skip)
      SFS_INSTRUMENTATION_SKIP="$2"
      shift
      ;;
    --sfs-output-directory)
      SFS_OUTPUT_DIRECTORY="$2"
      shift
      ;;
    --sfs-output-format)
      SFS_OUTPUT_FORMAT="$2"
      shift
      ;;
    --sfs-trace-fds)
      SFS_TRACE_FDS="$2"
      shift
      ;;
    *)
      echo "Invalid argument detected: $key"
      usage
      exit 1
  esac
  shift
done

BLOCKSIZE=${BLOCKSIZE:-134217728}
REPLICATION=${REPLICATION:-1}
MEMORY=${MEMORY:-61440}
CORES=${CORES:-16}
IO_BUFFER=${IO_BUFFER:-1048576}

export HADOOP_OPTS
# export HADOOP_OPTS="-XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps $HADOOP_OPTS"
export YARN_OPTS
# export YARN_OPTS="-XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps $YARN_OPTS"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_EXT"

# set up the environment variables
export HADOOP_PREFIX="$(pwd $(dirname $0))/.."
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_NODES=(`scontrol show hostnames`)
export HADOOP_NAMENODE=${HADOOP_NODES[0]}

# will become a sub-directory of /local
export HDFS_LOCAL_DIR=$USER/hdfs
export HDFS_LOCAL_LOG_DIR=$HDFS_LOCAL_DIR/log

if [ -z $COLOCATE_DATANODE_WITH_NAMENODE ]; then
  export HADOOP_DATANODES=(${HADOOP_NODES[@]:1})
else
  export HADOOP_DATANODES=(${HADOOP_NODES[@]:0})
fi

echo "$(date): Using Hadoop Distribution in '$HADOOP_PREFIX'."

echo "$(date): Starting Hadoop NameNode on '$HADOOP_NAMENODE' and DataNode(s) on '${HADOOP_DATANODES[@]}'."

cp $HADOOP_CONF_DIR/core-site.xml.template $HADOOP_CONF_DIR/core-site.xml
cp $HADOOP_CONF_DIR/hdfs-site.xml.template $HADOOP_CONF_DIR/hdfs-site.xml
cp $HADOOP_CONF_DIR/mapred-site.xml.template $HADOOP_CONF_DIR/mapred-site.xml
cp $HADOOP_CONF_DIR/yarn-site.xml.template $HADOOP_CONF_DIR/yarn-site.xml

echo "$(date): Creating configuration in '$HADOOP_CONF_DIR'."

printf "%s\n" "${HADOOP_DATANODES[@]}" > "${HADOOP_CONF_DIR}/slaves"

LOCAL=/local_ssd

# core configuration
cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${HADOOP_NAMENODE}:8020</value>
  </property>
<!--
  <property>
    <name>io.file.buffer.size</name>
    <value>${IO_BUFFER}</value>
  </property>
-->
  <property>
    <name>hadoop.tmp.dir</name>
    <value>$LOCAL/${HDFS_LOCAL_DIR}/tmp</value>
  </property>
EOF

# add SFS options if necessary
if [ -n "$SFS_WRAPPED_FS" ]; then
  cat >> $HADOOP_CONF_DIR/core-site.xml << EOF
  <property>
    <name>fs.sfs.impl</name>
    <value>de.zib.sfs.StatisticsFileSystem</value>
  </property>
  <property>
    <name>fs.AbstractFileSystem.sfs.impl</name>
    <value>de.zib.sfs.StatisticsFileSystemDelegate</value>
  </property>
  <property>
    <name>sfs.wrappedFS.className</name>
    <value>${SFS_WRAPPED_FS}</value>
  </property>
  <property>
    <name>sfs.wrappedFS.scheme</name>
    <value>${SFS_WRAPPED_SCHEME}</value>
  </property>
  <property>
    <name>sfs.instrumentation.skip</name>
    <value>${SFS_INSTRUMENTATION_SKIP}</value>
  </property>
  <property>
    <name>sfs.output.directory</name>
    <value>${SFS_OUTPUT_DIRECTORY}</value>
  </property>
  <property>
    <name>sfs.output.format</name>
    <value>${SFS_OUTPUT_FORMAT}</value>
  </property>
  <property>
    <name>sfs.traceFds</name>
    <value>${SFS_TRACE_FDS}</value>
  </property>
EOF
fi

# close configuration
cat >> $HADOOP_CONF_DIR/core-site.xml << EOF
</configuration>
EOF

# name node configuration
rm -rf $HADOOP_CONF_DIR/$HADOOP_NAMENODE
mkdir -p $HADOOP_CONF_DIR/$HADOOP_NAMENODE
export HADOOP_NAMENODE_HDFS_SITE=$HADOOP_CONF_DIR/$HADOOP_NAMENODE/hdfs-site.xml
cp $HADOOP_CONF_DIR/hdfs-site.xml $HADOOP_NAMENODE_HDFS_SITE

# cut off closing configuration tag
line_number=`grep -nr "</configuration>" "$HADOOP_NAMENODE_HDFS_SITE" | cut -d : -f 1`
printf '%s\n' "${line_number}s#.*##" w  | ed -s "$HADOOP_NAMENODE_HDFS_SITE"

cat >> $HADOOP_NAMENODE_HDFS_SITE << EOF
  <property>
    <name>dfs.namenode.rpc-address</name>
    <value>${HADOOP_NAMENODE}:8020</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://$LOCAL/${HDFS_LOCAL_DIR}/name</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>${BLOCKSIZE}</value>
  </property>
  <property>
    <name>dfs.namenode.fs-limits.min-block-size</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>${REPLICATION}</value>
  </property>
</configuration>
EOF

cp $HADOOP_NAMENODE_HDFS_SITE $HADOOP_CONF_DIR/hdfs-site.xml

# data node configurations
for datanode in ${HADOOP_DATANODES[@]}; do
  rm -rf $HADOOP_CONF_DIR/$datanode
  mkdir -p $HADOOP_CONF_DIR/$datanode
  hadoop_datanode_hdfs_site=$HADOOP_CONF_DIR/$datanode/hdfs-site.xml
  cp $HADOOP_CONF_DIR/hdfs-site.xml $hadoop_datanode_hdfs_site

  line_number=`grep -nr "</configuration>" "$hadoop_datanode_hdfs_site" | cut -d : -f 1`
  printf '%s\n' "${line_number}s#.*##" w  | ed -s "$hadoop_datanode_hdfs_site"

  cat >> $hadoop_datanode_hdfs_site << EOF
  <property>
    <name>dfs.datanode.address</name>
    <value>${datanode}:50010</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>${datanode}:50020</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://$LOCAL/${HDFS_LOCAL_DIR}/data</value>
  </property>
  <property>
    <name>dfs.datanode.transferTo.allowed</name>
    <value>true</value>
  </property>
</configuration>
EOF
done

# MapReduce configuration
cat > $HADOOP_CONF_DIR/mapred-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>${HADOOP_NAMENODE}:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>${HADOOP_NAMENODE}:19888</value>
  </property>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.client.submit.file.replication</name>
    <value>${REPLICATION}</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
<!--    <value>-Xmx2048M -XX:ErrorFile=/nfs/scratch/bzcschmi/hdfs-statistics-adapter/hs_err_pids/map_hs_err_pid%p.log -XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps $MAP_OPTS</value> -->
    <value>-Xmx2048M $MAP_OPTS</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>LD_LIBRARY_PATH=\${LD_LIBRARY_PATH}:${LD_LIBRARY_PATH_EXT}</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
<!--    <value>-Xmx2048M -XX:ErrorFile=/nfs/scratch/bzcschmi/hdfs-statistics-adapter/hs_err_pids/reduce_hs_err_pid%p.log -XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps $REDUCE_OPTS</value> -->
    <value>-Xmx2048M $REDUCE_OPTS</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>LD_LIBRARY_PATH=\${LD_LIBRARY_PATH}:${LD_LIBRARY_PATH_EXT}</value>
  </property>
<!-- -->
  <property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>1024</value>
  </property>
<!-- -->
  <property>
    <name>yarn.app.mapreduce.am.command-opts</name>
<!--    <value>-Xmx1024M -XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps $MRAPPMASTER_OPTS</value> -->
    <value>-Xmx1024M $MRAPPMASTER_OPTS</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>LD_LIBRARY_PATH=\${LD_LIBRARY_PATH}:${LD_LIBRARY_PATH_EXT}</value>
  </property>
<!--
  <property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>32</value>
  </property>
  <property>
    <name>mapreduce.shuffle.transferTo.allowed</name>
    <value>true</value>
  </property>
  <property>
    <name>mapreduce.shuffle.transfer.buffer.size</name>
    <value>81920</value>
  </property>
-->
<!-- -->
  <property>
    <name>mapreduce.reduce.merge.inmem.threshold</name>
    <value>0</value>
  </property>
  <property>
    <name>mapreduce.reduce.input.buffer.percent</name>
    <value>0.5</value>
  </property>
<!-- -->
<!--
  <property>
    <name>mapreduce.map.speculative</name>
    <value>true</value>
  </property>
  <property>
    <name>mapreduce.reduce.speculative</name>
    <value>true</value>
  </property>
-->
</configuration>
EOF

# YARN configuration
cat > $HADOOP_CONF_DIR/yarn-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<!--
  <property>
    <name>yarn.nodemanager.delete.debug-delay-sec</name>
    <value>-1</value>
  </property>
-->
<!--
  <property>
    <name>yarn.am.liveness-monitor.expiry-interval-ms</name>
    <value>60000</value>
  </property>
-->
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.nodemanager.sleep-delay-before-sigkill.ms</name>
    <value>60000</value>
  </property>
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>${MEMORY}</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>${MEMORY}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${HADOOP_NAMENODE}</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>${CORES}</value>
  </property>
  <property>
    <name>yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage</name>
    <value>99.0</value>
  </property>
</configuration>
EOF

cp $HADOOP_CONF_DIR/yarn-site.xml $HADOOP_CONF_DIR/$HADOOP_NAMENODE/yarn-site.xml

for datanode in ${HADOOP_DATANODES[@]}; do
  datanode_yarn_site=$HADOOP_CONF_DIR/$datanode/yarn-site.xml
  cp $HADOOP_CONF_DIR/yarn-site.xml $datanode_yarn_site

  line_number=`grep -nr "</configuration>" "$datanode_yarn_site" | cut -d : -f 1`
  printf '%s\n' "${line_number}s#.*##" w | ed -s "$datanode_yarn_site"

  cat >> $datanode_yarn_site << EOF
  <property>
    <name>yarn.nodemanager.hostname</name>
    <value>${datanode}</value>
  </property>
</configuration>
EOF

done

# start name node
mkdir -p /local/$HDFS_LOCAL_DIR
mkdir -p /local_ssd/$HDFS_LOCAL_DIR
mkdir -p /local/${HDFS_LOCAL_DIR}/tmp
mkdir -p /local_ssd/${HDFS_LOCAL_DIR}/tmp
mkdir -p /local/$HDFS_LOCAL_LOG_DIR
mkdir -p /local_ssd/$HDFS_LOCAL_LOG_DIR

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$HADOOP_NAMENODE:$HADOOP_CLASSPATH"

export HDFS_NAMENODE_LOG=$LOCAL/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log
# export HDFS_NAMENODE_LOG=/local_ssd/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log

echo "$(date): Formatting NameNode."
ulimit -c unlimited
# the scripts asks for confirmation
$HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode -format >> $HDFS_NAMENODE_LOG 2>&1 << EOF
Y
EOF
echo "$(date): Formatting NameNode done."

echo "$(date): Starting NameNode on $(hostname)."
nohup $HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode >> $HDFS_NAMENODE_LOG 2>&1 &
echo $! > $LOCAL/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
# echo $! > /local_ssd/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
echo "$(date): Starting NameNode done (PID file: $LOCAL/$HDFS_LOCAL_DIR/namenode-$(hostname).pid)."
# echo "$(date): Starting NameNode done (PID file: /local_ssd/$HDFS_LOCAL_DIR/namenode-$(hostname).pid)."

for datanode in ${HADOOP_DATANODES[@]}; do
  datanode_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-start-datanode.sh
  cat > $datanode_script << EOF
#!/bin/bash

# same as for namenode
mkdir -p /local/$HDFS_LOCAL_DIR
mkdir -p /local_ssd/$HDFS_LOCAL_DIR
mkdir -p /local/${HDFS_LOCAL_DIR}/tmp
mkdir -p /local_ssd/${HDFS_LOCAL_DIR}/tmp
mkdir -p /local/$HDFS_LOCAL_LOG_DIR
mkdir -p /local_ssd/$HDFS_LOCAL_LOG_DIR

export HADOOP_OPTS="$HADOOP_OPTS"
export HADOOP_HEAPSIZE=2000
export LD_LIBRARY_PATH="\$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_EXT"
export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$datanode:$HADOOP_CLASSPATH"
export HDFS_DATANODE_LOG=$LOCAL/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log
# export HDFS_DATANODE_LOG=/local_ssd/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log
#export JSTAT_LOG=/local/$HDFS_LOCAL_LOG_DIR/datanode-jstat-$datanode.log

nohup $HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR datanode >> \$HDFS_DATANODE_LOG 2>&1 &
pid=\$!
echo \$pid > $LOCAL/$HDFS_LOCAL_DIR/datanode-$datanode.pid
# echo \$pid > /local_ssd/$HDFS_LOCAL_DIR/datanode-$datanode.pid
#nohup jstat -gcutil \$pid 5000 >> \$JSTAT_LOG 2>&1 &
#echo \$! > /local/$HDFS_LOCAL_DIR/datanode-jstat-$datanode.pid
EOF
  chmod +x $datanode_script
  echo "$(date): Starting DataNode on $datanode."
  srun --nodes=1-1 --nodelist=$datanode $datanode_script
  echo "$(date): Starting DataNode on $datanode done."
  rm $datanode_script
done

# start resource manager
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$(hostname)"
export YARN_RESOURCEMANAGER_LOG=$LOCAL/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log
# export YARN_RESOURCEMANAGER_LOG=/local_ssd/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log

echo "$(date): Starting ResourceManager on $(hostname)."
nohup $HADOOP_PREFIX/bin/yarn --config $HADOOP_CONF_DIR resourcemanager >> $YARN_RESOURCEMANAGER_LOG 2>&1 &
echo $! > $LOCAL/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
# echo $! > /local_ssd/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
echo "$(date): Starting ResourceManager done (PID file $LOCAL/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid)."
# echo "$(date): Starting ResourceManager done (PID file /local_ssd/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid)."

for datanode in ${HADOOP_DATANODES[@]}; do
nodemanager_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-start-nodemanager.sh
  cat > $nodemanager_script << EOF
#!/bin/bash

# same as for resource manager
export YARN_OPTS="$YARN_OPTS"
export YARN_HEAPSIZE=2000
export LD_LIBRARY_PATH="\$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_EXT"
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$datanode"
export YARN_NODEMANAGER_LOG=$LOCAL/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log
# export YARN_NODEMANAGER_LOG=/local_ssd/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log
#export JSTAT_LOG=/local/$HDFS_LOCAL_LOG_DIR/nodemanager-jstat-$datanode.log

nohup $HADOOP_PREFIX/bin/yarn --config $HADOOP_CONF_DIR nodemanager >> \$YARN_NODEMANAGER_LOG 2>&1 &
pid=\$!
echo \$pid > $LOCAL/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
# echo \$pid > /local_ssd/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
#nohup jstat -gcutil \$pid 5000 >> \$JSTAT_LOG 2>&1 &
#echo \$! > /local/$HDFS_LOCAL_DIR/nodemanager-jstat-$datanode.pid
EOF
  chmod +x $nodemanager_script
  echo "$(date): Starting NodeManager on $datanode."
  srun --nodes=1-1 --nodelist=$datanode $nodemanager_script
  echo "$(date): Starting NodeManager on $datanode done."
  rm $nodemanager_script
done

export JOBHISTORY_SERVER_LOG=$LOCAL/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log
# export JOBHISTORY_SERVER_LOG=/local_ssd/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log

echo "$(date): Starting JobHistory Server on $(hostname)."
nohup $HADOOP_PREFIX/bin/mapred --config $HADOOP_CONF_DIR historyserver >> $JOBHISTORY_SERVER_LOG 2>&1 &
echo $! > $LOCAL/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
# echo $! > /local_ssd/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
echo "$(date): Starting JobHistory Server done (PID file $LOCAL/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid)."
# echo "$(date): Starting JobHistory Server done (PID file /local_ssd/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid)."

echo "$(date): Starting Hadoop done."
