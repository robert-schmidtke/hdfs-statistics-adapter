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
export HADOOP_PREFIX="$(pwd $(dirname $0))/.."
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

echo "$(date): Stopping NameNode."
pidfile=/local/$HDFS_LOCAL_DIR/namenode-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if [ -e /proc/$pid ]; then
    kill $pid
    echo "$(date): Waiting for process $pid to stop after kill: $?"
    while [ -e /proc/$pid ]; do sleep 1s; done
    echo "$(date): Process $pid stopped"
  fi
  rm $pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/namenode-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/namenode-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping NameNode done."

for datanode in ${HADOOP_DATANODES[@]}; do
  echo "$(date): Stopping DataNode on $datanode."
  datanode_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-datanode.sh

  cat > $datanode_script << EOF
#!/bin/bash

pidfile=/local/$HDFS_LOCAL_DIR/datanode-$datanode.pid
if [ -f \$pidfile ]; then
  pid=\$(cat \$pidfile)
  if [ -e /proc/\$pid ]; then
    kill \$pid
    echo "\$(date): Waiting for process \$pid to stop after kill: \$?"
    while [ -e /proc/\$pid ]; do sleep 1s; done
    echo "\$(date): Process \$pid stopped"
  else
    echo "DataNode PID \$pid does not exist"
  fi
  rm \$pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/datanode-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/datanode-$datanode.log
else
  echo "DataNode PID file \$pidfile does not exist."
fi
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
pidfile=/local/$HDFS_LOCAL_DIR/resourcemanager-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if [ -e /proc/$pid ]; then
    kill $pid
    echo "$(date): Waiting for process $pid to stop after kill: $?"
    while [ -e /proc/$pid ]; do sleep 1s; done
    echo "$(date): Process $pid stopped"
  fi
  rm $pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/resourcemanager-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/resourcemanager-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping ResourceManager done."

echo "$(date): Stopping JobHistory Server."
pidfile=/local/$HDFS_LOCAL_DIR/jobhistory_server-$(hostname).pid
if [ -f $pidfile ]; then
  pid=`cat $pidfile`
  if [ -e /proc/$pid ]; then
    kill $pid
    echo "$(date): Waiting for process $pid to stop after kill: $?"
    while [ -e /proc/$pid ]; do sleep 1s; done
    echo "$(date): Process $pid stopped"
  fi
  rm $pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/jobhistory_server-$(hostname).log $HADOOP_PREFIX/log-$SLURM_JOB_ID/jobhistory_server-$(hostname).log
else
  echo "PID file $pidfile does not exist."
fi
echo "Stopping JobHistory Server done."

for datanode in ${HADOOP_DATANODES[@]}; do
  echo "$(date): Stopping NodeManager on $datanode."
  nodemanager_script=$(dirname $0)/${SLURM_JOB_ID}-${datanode}-stop-nodemanager.sh

  cat > $nodemanager_script << EOF
#!/bin/bash

pidfile=/local/$HDFS_LOCAL_DIR/nodemanager-$datanode.pid
if [ -f \$pidfile ]; then
  pid=\$(cat \$pidfile)
  if [ -e /proc/\$pid ]; then
    kill \$pid
    echo "\$(date): Waiting for process \$pid to stop after kill: \$?"
    while [ -e /proc/\$pid ]; do sleep 1s; done
    echo "\$(date): Process \$pid stopped"
  fi
  rm \$pidfile
  cp /local/$HDFS_LOCAL_LOG_DIR/nodemanager-$datanode.log $HADOOP_PREFIX/log-$SLURM_JOB_ID/nodemanager-$datanode.log
  rm -rf /local/$HDFS_LOCAL_LOG_DIR
  rm -rf /local/$HDFS_LOCAL_DIR
else
  echo "PID file \$pidfile does not exist."
fi
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
rm -rf /local/$HDFS_LOCAL_DIR

echo "Stopping Hadoop done."
