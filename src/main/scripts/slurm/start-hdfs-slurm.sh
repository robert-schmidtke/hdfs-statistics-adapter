#!/bin/bash

usage() {
  echo "Usage: srun --nodes=1-1 --nodelist<NAMENODE> start-hdfs-slurm.sh"
  echo "  -b|--blocksize <block size in bytes> (default: 134217728)"
  echo "  -r|--replication <replication factor> (default: 1)"
  echo "  -m|--memory <total memory to be used on each node in megabytes> (default: 61440)"
  echo "  -o|--cores <total number of cores to be used on each node> (default: 16)"
  echo "  -i|--io-buffer <size of the IO buffer in bytes> (default: 1048576)"
  echo "  -c|--colocate-datanode-with-namenode (default: not specified/false)"
  echo "SFS specific options (default: not specified/do not use SFS):"
  echo "     --sfs-wrapped-fs <wrapped file system class name> (default: not specified; enables SFS if specified)"
  echo "     --sfs-wrapped-scheme <scheme of the wrapped file system> (default: not specified)"
  echo "     --sfs-logfilename <node-local path to a log file> (default: /tmp/sfs/async.log)"
  echo "     --sfs-delete-on-close (default: not specified/false)"
  echo "     --sfs-target-logfile-directory <path to a shared folder to store the node-local log in after close> (default: not specified)"
}

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected."
  usage
  exit 1
fi

while [[ $# -gt 1 ]]; do
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
    --sfs-wrapped-fs)
      SFS_WRAPPED_FS="$2"
      shift
      ;;
    --sfs-wrapped-scheme)
      SFS_WRAPPED_SCHEME="$2"
      shift
      ;;
    --sfs-logfilename)
      SFS_LOGFILENAME="$2"
      shift
      ;;
    --sfs-delete-on-close)
      SFS_DELETE_ON_CLOSE="true"
      ;;
    --sfs-target-logfile-directory)
      SFS_TARGET_LOGFILE_DIRECTORY="$2"
      shift
      ;;
    *)
      echo "Invalid argument detected."
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
SFS_LOGFILENAME=${SFS_LOGFILENAME:-/tmp/sfs/async.log}

# set up the environment variables
export HADOOP_PREFIX="$(pwd $(dirname $0))/.."
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
HADOOP_NODES=(`scontrol show hostnames`)
export NODES
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

# core configuration
cat > $HADOOP_CONF_DIR/core-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${HADOOP_NAMENODE}:8020</value>
  </property>
  <property>
    <name>io.file.buffer.size</name>
    <value>${IO_BUFFER}</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/local/${HDFS_LOCAL_DIR}/tmp</value>
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
    <name>sfs.logFile.name</name>
    <value>${SFS_LOGFILENAME}</value>
  </property>
EOF

  # optional delete on close option
  if [ -n "$SFS_DELETE_ON_CLOSE" ]; then
    cat >> $HADOOP_CONF_DIR/core-site.xml << EOF
  <property>
    <name>sfs.logFile.deleteOnClose</name>
    <value>true</value>
  </property>
EOF
  fi

  # optional target log file directory option
  if [ -n "$SFS_TARGET_LOGFILE_DIRECTORY" ]; then
    cat >> $HADOOP_CONF_DIR/core-site.xml << EOF
  <property>
    <name>sfs.targetLogFileDirectory</name>
    <value>${SFS_TARGET_LOGFILE_DIRECTORY}</value>
  </property>
EOF
  fi
fi

# close configuration
cat >> $HADOOP_CONF_DIR/core-site.xml << EOF
</configuration>
EOF

# name node configuration
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
    <value>file:///local/${HDFS_LOCAL_DIR}/name</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>${BLOCKSIZE}</value>
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
    <value>file:///local/${HDFS_LOCAL_DIR}/data</value>
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
    <value>3072</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx2048M</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>4096</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx3072M</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.mb</name>
    <value>1024</value>
  </property>
  <property>
    <name>mapreduce.task.io.sort.factor</name>
    <value>32</value>
  </property>
</configuration>
EOF

# YARN configuration
cat > $HADOOP_CONF_DIR/yarn-site.xml << EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
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
mkdir -p /local/${HDFS_LOCAL_DIR}/tmp
mkdir -p /local/$HDFS_LOCAL_LOG_DIR

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$HADOOP_NAMENODE:$HADOOP_CLASSPATH"

export HDFS_NAMENODE_LOG=/local/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log

echo "$(date): Formatting NameNode."
ulimit -c unlimited
# the scripts asks for confirmation
$HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode -format >> $HDFS_NAMENODE_LOG 2>&1 << EOF
Y
EOF
echo "$(date): Formatting NameNode done."

echo "$(date): Starting NameNode on $(hostname)."
nohup $HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR namenode >> $HDFS_NAMENODE_LOG 2>&1 &
echo $! > /local/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
echo "$(date): Starting NameNode done (PID file: /local/$HDFS_LOCAL_DIR/namenode-$(hostname).pid)."

for datanode in ${HADOOP_DATANODES[@]}; do
  datanode_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-start-datanode.sh
  cat > $datanode_script << EOF
#!/bin/bash

# same as for namenode
mkdir -p /local/$HDFS_LOCAL_DIR
mkdir -p /local/${HDFS_LOCAL_DIR}/tmp
mkdir -p /local/$HDFS_LOCAL_LOG_DIR

export HADOOP_USER_CLASSPATH_FIRST="YES"
export HADOOP_CLASSPATH="$HADOOP_CONF_DIR/$datanode:$HADOOP_CLASSPATH"
export HDFS_DATANODE_LOG=/local/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log
#export JSTAT_LOG=/local/$HDFS_LOCAL_LOG_DIR/datanode-jstat-$datanode.log

nohup $HADOOP_PREFIX/bin/hdfs --config $HADOOP_CONF_DIR datanode >> \$HDFS_DATANODE_LOG 2>&1 &
pid=\$!
echo \$pid > /local/$HDFS_LOCAL_DIR/datanode-$datanode.pid
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
export YARN_RESOURCEMANAGER_LOG=/local/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log

echo "$(date): Starting ResourceManager on $(hostname)."
nohup $HADOOP_PREFIX/bin/yarn --config $HADOOP_CONF_DIR resourcemanager >> $YARN_RESOURCEMANAGER_LOG 2>&1 &
echo $! > /local/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
echo "$(date): Starting ResourceManager done (PID file /local/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid)."

for datanode in ${HADOOP_DATANODES[@]}; do
nodemanager_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-start-nodemanager.sh
  cat > $nodemanager_script << EOF
#!/bin/bash

# same as for resource manager
export YARN_USER_CLASSPATH="$YARN_USER_CLASSPATH:$HADOOP_CONF_DIR/$datanode"
export YARN_NODEMANAGER_LOG=/local/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log
#export JSTAT_LOG=/local/$HDFS_LOCAL_LOG_DIR/nodemanager-jstat-$datanode.log

nohup $HADOOP_PREFIX/bin/yarn --config $HADOOP_CONF_DIR nodemanager >> \$YARN_NODEMANAGER_LOG 2>&1 &
pid=\$!
echo \$pid > /local/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
#nohup jstat -gcutil \$pid 5000 >> \$JSTAT_LOG 2>&1 &
#echo \$! > /local/$HDFS_LOCAL_DIR/nodemanager-jstat-$datanode.pid
EOF
  chmod +x $nodemanager_script
  echo "$(date): Starting NodeManager on $datanode."
  srun --nodes=1-1 --nodelist=$datanode $nodemanager_script
  echo "$(date): Starting NodeManager on $datanode done."
  rm $nodemanager_script
done

export JOBHISTORY_SERVER_LOG=/local/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log

echo "$(date): Starting JobHistory Server on $(hostname)."
nohup $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver >> $JOBHISTORY_SERVER_LOG 2>&1 &
echo $! > /local/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
echo "$(date): Starting JobHistory Server done (PID file /local/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid)."

echo "$(date): Starting Hadoop done."
