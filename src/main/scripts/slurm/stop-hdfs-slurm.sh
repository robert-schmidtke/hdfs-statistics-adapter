#!/bin/bash

USAGE="Usage: srun --nodes=1-1 --nodelist=<NAMENODE> stop-hdfs-slurm.sh"

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected. $USAGE"
  exit 1
fi

# set up the environment variables
export HADOOP_PREFIX="$(pwd $(dirname $0))/.."
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
HADOOP_NODES=(`scontrol show hostnames`)
export NODES
export HADOOP_NAMENODE=${HADOOP_NODES[0]}
export HADOOP_DATANODES=(${HADOOP_NODES[@]:1})
export HDFS_LOCAL_DIR=$USER/hdfs
export HDFS_LOCAL_LOG_DIR=$HDFS_LOCAL_DIR/log

echo "Using Hadoop Distribution in '$HADOOP_PREFIX'."

echo "Stopping Hadoop NameNode on '$HADOOP_NAMENODE' and DataNode(s) on '${HADOOP_DATANODES[@]}'."

mkdir -p $HADOOP_PREFIX/log-$SLURM_JOB_ID

echo "Stopping NameNode."
pidfile=/local/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if [ -e /proc/$pid ]; then
    kill -9 $pid
    echo "Result: $?"
  fi
  rm $pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/namenode-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping NameNode done."

for datanode in ${HADOOP_DATANODES[@]}; do
  echo "Stopping DataNode on $datanode."
  datanode_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-datanode.sh

  cat > $datanode_script << EOF
#!/bin/bash

pidfile=/local/$HDFS_LOCAL_DIR/datanode-$datanode.pid
if [ -f \$pidfile ]; then
  pid=\$(cat \$pidfile)
  if [ -e /proc/\$pid ]; then
    kill -9 \$pid
    echo "Result: \$?"
  else
    echo "DataNode PID \$pid does not exist"
  fi
  rm \$pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/datanode-$datanode.log
else
  echo "DataNode PID file \$pidfile does not exist."
fi

#pidfile=/local/$HDFS_LOCAL_DIR/datanode-jstat-$datanode.pid
#if [ -f \$pidfile ]; then
#  pid=\$(cat \$pidfile)
#  if [ -e /proc/\$pid ]; then
#    kill -9 \$pid
#    echo "Result: \$?"
#  else
#    echo "jstat PID \$pid does not exist"
#  fi
#  rm \$pidfile
#  cp /local/$HDFS_LOCAL_LOG_DIR/datanode-jstat-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/datanode-jstat-$datanode.log
#else
#  echo "jstat PID file \$pidfile does not exist."
#fi
EOF

  chmod +x $datanode_script
  srun --nodes=1-1 --nodelist=$datanode $datanode_script
  echo "Stopping DataNode on $datanode done."
  rm $datanode_script
done

echo "Stopping ResourceManager."
pidfile=/local/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if [ -e /proc/$pid ]; then
    kill -9 $pid
    echo "Result: $?"
  fi
  rm $pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/resourcemanager-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping ResourceManager done."

echo "Stopping JobHistory Server."
pidfile=/local/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if [ -e /proc/$pid ]; then
    kill -9 $pid
    echo "Result: $?"
  fi
  rm $pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/jobhistory_server-$(hostname).log
  rm -rf /local/$HDFS_LOCAL_LOG_DIR
  rm -rf /local/$HDFS_LOCAL_DIR
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping JobHistory Server done."

for datanode in ${HADOOP_DATANODES[@]}; do
  nodemanager_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-datanode.sh
  cat > $nodemanager_script << EOF
#!/bin/bash

pidfile=/local/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
if [ -f \$pidfile ]; then
  pid=\$(cat \$pidfile)
  if [ -e /proc/\$pid ]; then
    kill -9 \$pid
    echo "Result: \$?"
  fi
  rm \$pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/nodemanager-$datanode.log
  rm -rf /local/$HDFS_LOCAL_LOG_DIR
  rm -rf /local/$HDFS_LOCAL_DIR
else
  echo "PID file \$pidfile does not exist."
fi

#pidfile=/local/$HDFS_LOCAL_DIR/nodemanager-jstat-$datanode.pid
#if [ -f \$pidfile ]; then
#  pid=\$(cat \$pidfile)
#  if [ -e /proc/\$pid ]; then
#    kill -9 \$pid
#    echo "Result: \$?"
#  fi
#  rm \$pidfile
#  cp /local/$HDFS_LOCAL_LOG_DIR/nodemanager-jstat-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/nodemanager-jstat-$datanode.log
#  rm -rf /local/$HDFS_LOCAL_LOG_DIR
#  rm -rf /local/$HDFS_LOCAL_DIR
#else
#  echo "jstat PID file \$pidfile does not exist."
#fi
EOF
  chmod +x $nodemanager_script

  echo "Stopping NodeManager on $datanode."
  srun --nodes=1-1 --nodelist=$datanode $nodemanager_script
  echo "Stopping NodeManager on $datanode done."
  rm $nodemanager_script
done

echo "Stopping Hadoop done."
