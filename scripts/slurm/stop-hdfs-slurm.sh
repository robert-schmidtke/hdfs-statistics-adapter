#!/bin/bash

usage() {
  echo "Usage: srun --nodes=1-1 --nodelist=<NAMENODE> stop-hdfs-slurm.sh"
  echo "  -c|--colocate-datanode-with-namenode (default: not specified/false)"
}

if [ -z $SLURM_JOB_ID ]; then
  echo "No Slurm environment detected."
  usage
  exit 1
fi

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -c|--colocate-datanode-with-namenode)
      COLOCATE_DATANODE_WITH_NAMENODE="true"
      ;;
    *)
      echo "Invalid argument detected."
      usage
      exit 1
  esac
  shift
done

# set up the environment variables
export HADOOP_PREFIX="$(realpath $(pwd $(dirname $0))/..)"
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_NODES=(`scontrol show hostnames`)
export HADOOP_NAMENODE=${HADOOP_NODES[0]}

export HDFS_LOCAL_DIR=$USER/hdfs
export HDFS_LOCAL_LOG_DIR=$HDFS_LOCAL_DIR/log

if [ -z $COLOCATE_DATANODE_WITH_NAMENODE ]; then
  export HADOOP_DATANODES=(${HADOOP_NODES[@]:1})
else
  export HADOOP_DATANODES=(${HADOOP_NODES[@]:0})
fi

echo "Using Hadoop Distribution in '$HADOOP_PREFIX'."

echo "Stopping Hadoop NameNode on '$HADOOP_NAMENODE' and DataNode(s) on '${HADOOP_DATANODES[@]}'."

mkdir -p $HADOOP_PREFIX/log-$SLURM_JOB_ID

LOCAL=/local_ssd

echo "$(date): Stopping NameNode."
pidfile=$LOCAL/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
# pidfile=/local_ssd/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if kill -0 $pid > /dev/null 2>&1; then
    received_signal=1
    while [ "$received_signal" -eq "1" ]; do
      echo "Sending SIGTERM to NameNode $pid"
      kill $pid
      sleep 5s
      grep "RECEIVED SIGNAL 15: SIGTERM" $LOCAL/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log > /dev/null 2>&1
#       grep "RECEIVED SIGNAL 15: SIGTERM" /local_ssd/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log > /dev/null 2>&1
      received_signal=$?
    done

    echo "Waiting for NameNode $pid to shut down"
    while [ -e /proc/$pid ]; do sleep 1s; done
    echo "NameNode $pid shut down"
  else
    echo "NameNode $pid is not running"
  fi
  rm $pidfile
  cp $LOCAL/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/namenode-$(hostname).log
#   cp /local_ssd/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/namenode-$(hostname).log
else
  echo "NameNode PID file $pidfile does not exist."
fi
echo "Stopping NameNode done."

for datanode in ${HADOOP_DATANODES[@]}; do
  echo "$(date): Stopping DataNode on $datanode."
  datanode_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-datanode.sh

  cat > $datanode_script << EOF
#!/bin/bash

pidfile=$LOCAL/$HDFS_LOCAL_DIR/datanode-$datanode.pid
# pidfile=/local_ssd/$HDFS_LOCAL_DIR/datanode-$datanode.pid
if [ -f \$pidfile ]; then
  pid=\$(cat \$pidfile)
  if kill -0 \$pid > /dev/null 2>&1; then
    received_signal=1
    while [ "\$received_signal" -eq "1" ]; do
      echo "Sending SIGTERM to DataNode \$pid"
      kill \$pid
      sleep 5s
      grep "RECEIVED SIGNAL 15: SIGTERM" $LOCAL/$HDFS_LOCAL_LOG_DIR/datanode-\$(hostname).log > /dev/null 2>&1
#       grep "RECEIVED SIGNAL 15: SIGTERM" /local_ssd/$HDFS_LOCAL_LOG_DIR/datanode-\$(hostname).log > /dev/null 2>&1
      received_signal=\$?
    done

    echo "Waiting for DataNode \$pid to shut down"
    while [ -e /proc/\$pid ]; do sleep 1s; done
    echo "DataNode \$pid shut down"
  else
    echo "DataNode \$pid is not running"
  fi
  rm \$pidfile
  cp $LOCAL/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/datanode-$datanode.log
#   cp /local_ssd/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/datanode-$datanode.log
else
  echo "DataNode PID file \$pidfile does not exist."
fi

#pidfile=/local/$HDFS_LOCAL_DIR/datanode-jstat-$datanode.pid
#if [ -f \$pidfile ]; then
#  pid=\$(cat \$pidfile)
#  if [ -e /proc/\$pid ]; then
#    kill \$pid
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
  srun --nodes=1-1 --nodelist=$datanode $datanode_script &
done

echo "$(date): Waiting for all DataNodes to stop"
wait
for datanode in ${HADOOP_DATANODES[@]}; do
  echo "$(date): Stopping DataNode on $datanode done."
  rm $(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-datanode.sh
done

echo "$(date): Stopping ResourceManager."
pidfile=$LOCAL/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
# pidfile=/local_ssd/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if kill -0 $pid > /dev/null 2>&1; then
    received_signal=1
    while [ "$received_signal" -eq "1" ]; do
      echo "Sending SIGTERM to ResourceManager $pid"
      kill $pid
      sleep 5s
      grep "RECEIVED SIGNAL 15: SIGTERM" $LOCAL/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log > /dev/null 2>&1
#       grep "RECEIVED SIGNAL 15: SIGTERM" /local_ssd/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log > /dev/null 2>&1
      received_signal=$?
    done

    echo "Waiting for ResourceManager $pid to shut down"
    while [ -e /proc/$pid ]; do sleep 1s; done
    echo "ResourceManager $pid shut down"
  else
    echo "ResourceManager $pid is not running"
  fi
  rm $pidfile
  cp $LOCAL/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/resourcemanager-$(hostname).log
#   cp /local_ssd/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/resourcemanager-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping ResourceManager done."

echo "$(date): Stopping JobHistory Server."
pidfile=$LOCAL/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
# pidfile=/local_ssd/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if kill -0 $pid > /dev/null 2>&1; then
    received_signal=1
    while [ "$received_signal" -eq "1" ]; do
      echo "Sending SIGTERM to JobHistory Server $pid"
      kill $pid
      sleep 5s
      grep "RECEIVED SIGNAL 15: SIGTERM" $LOCAL/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log > /dev/null 2>&1
#       grep "RECEIVED SIGNAL 15: SIGTERM" /local_ssd/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log > /dev/null 2>&1
      received_signal=$?
    done

    echo "Waiting for JobHistory Server $pid to shut down"
    while [ -e /proc/$pid ]; do sleep 1s; done
    echo "JobHistory Server $pid shut down"
  else
    echo "JobHistory Server $pid is not running"
  fi
  rm $pidfile
  cp $LOCAL/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/jobhistory_server-$(hostname).log
#   cp /local_ssd/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/jobhistory_server-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping JobHistory Server done."

for datanode in ${HADOOP_DATANODES[@]}; do
  echo "$(date): Stopping NodeManager on $datanode."
  nodemanager_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-nodemanager.sh

  cat > $nodemanager_script << EOF
#!/bin/bash

pidfile=$LOCAL/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
# pidfile=/local_ssd/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
if [ -f \$pidfile ]; then
  pid=\$(cat \$pidfile)
  if kill -0 \$pid > /dev/null 2>&1; then
    received_signal=1
    while [ "\$received_signal" -eq "1" ]; do
      echo "Sending SIGTERM to NodeManager \$pid"
      kill \$pid
      sleep 5s
      grep "RECEIVED SIGNAL 15: SIGTERM" $LOCAL/$HDFS_LOCAL_LOG_DIR/nodemanager-\$(hostname).log > /dev/null 2>&1
#       grep "RECEIVED SIGNAL 15: SIGTERM" /local_ssd/$HDFS_LOCAL_LOG_DIR/nodemanager-\$(hostname).log > /dev/null 2>&1
      received_signal=\$?
    done

    echo "Waiting for NodeManager \$pid to shut down"
    while [ -e /proc/\$pid ]; do sleep 1s; done
    echo "NodeManager \$pid shut down"
  else
    echo "NodeManager \$pid is not running"
  fi
  rm \$pidfile
  cp $LOCAL/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/nodemanager-$datanode.log
#   cp /local_ssd/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/nodemanager-$datanode.log
  rm -rf /local/$HDFS_LOCAL_LOG_DIR
  rm -rf /local_ssd/$HDFS_LOCAL_LOG_DIR
  rm -rf /local/$HDFS_LOCAL_DIR
  rm -rf /local_ssd/$HDFS_LOCAL_DIR
else
  echo "PID file \$pidfile does not exist."
fi

#pidfile=/local/$HDFS_LOCAL_DIR/nodemanager-jstat-$datanode.pid
#if [ -f \$pidfile ]; then
#  pid=\$(cat \$pidfile)
#  if [ -e /proc/\$pid ]; then
#    kill \$pid
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
  srun --nodes=1-1 --nodelist=$datanode $nodemanager_script &
done

echo "$(date): Waiting for all NodeManagers to stop"
wait
for datanode in ${HADOOP_DATANODES[@]}; do
  echo "$(date): Stopping NodeManager on $datanode done."
  rm $(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-nodemanager.sh
done

rm -rf /local/$HDFS_LOCAL_LOG_DIR
rm -rf /local_ssd/$HDFS_LOCAL_LOG_DIR
rm -rf /local/$HDFS_LOCAL_DIR
rm -rf /local_ssd/$HDFS_LOCAL_DIR

echo "Stopping Hadoop done."
