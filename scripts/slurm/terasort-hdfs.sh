#!/bin/bash

#SBATCH -J terasort-hdfs
#SBATCH --exclusive
#SBATCH --open-mode=append

usage() {
  echo "Usage: sbatch --nodes=<NODES> flink-terasort.sh"
  echo "  -e|--engine <flink|hadoop> (default: not specified)"
}

while [[ $# -gt 1 ]]; do
  key="$1"
  case $key in
    -e|--engine)
      ENGINE="$2"
      shift
      ;;
    *)
      echo "Invalid argument detected."
      usage
      exit 1
  esac
  shift
done

if [ -n "$ENGINE" ]; then
  case $ENGINE in
    flink|hadoop)
      echo "Using engine: $ENGINE"
      ;;
    *)
      echo "Invalid engine specified."
      usage
      exit 1
  esac
else
  echo "No engine specified."
  usage
  exit 1
fi

if [ -f /etc/debian_version ]
  then
    function module { eval `/usr/bin/modulecmd bash $*`; }
    export MODULEPATH=/dassw/ubuntu/modules
fi

module load java/oracle-jdk1.8.0_45

export HOSTNAME=$(hostname)

export FLINK_HOME=/scratch/$USER/flink-1.1.3

NODES=(`scontrol show hostnames`)
export NODES
export MASTER=${NODES[0]}

echo "Nodes: ${NODES[@]}"

export HADOOP_VERSION=2.7.3
export HADOOP_HOME=/scratch/$USER/hadoop-${HADOOP_VERSION}
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HDFS_LOCAL_DIR=$USER/hdfs
export HDFS_LOCAL_LOG_DIR=$HDFS_LOCAL_DIR/log

export GRPC_HOME=/scratch/$USER/grpc

export SFS_DIRECTORY=/scratch/$USER/hdfs-statistics-adapter
export SFS_TARGET_DIRECTORY=/scratch/$USER/statistics-fs/logs

export TERASORT_DIRECTORY=/scratch/$USER/terasort

echo "$(date): Cleaning Java processes"
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "$(date): Cleaning Java processes done"

# Flink's temporary directories are set by YARN if using HDFS

echo "$(date): Cleaning local directories"
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/hdfs
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/sfs
echo "$(date): Cleaning local directories done"

echo "$(date): Creating local folders"
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/hdfs
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/sfs
echo "$(date): Creating local folders done"

echo "$(date): Starting HDFS"
rm -rf $HADOOP_HOME/logs/*
cp ./start-hdfs-slurm.sh $HADOOP_HOME/sbin

# 256M block size, replication factor of 1, 50G total node memory for YARN, put first datanode on namenode host
SRUN_STANDARD_OPTS="--nodelist=$MASTER --nodes=1-1 --chdir=$HADOOP_HOME/sbin"
HDFS_STANDARD_OPTS="--blocksize 268435456 --replication 1 --memory 51200 --cores 16 --io-buffer 1048576 --colocate-datanode-with-namenode"
HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --hadoop-opts -agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,log_file_name=/local/$USER/sfs/sfs.log.hadoop"
HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --map-opts -agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,log_file_name=/local/$USER/sfs/sfs.log.map"
HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --reduce-opts -agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,log_file_name=/local/$USER/sfs/sfs.log.reduce"
HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --yarn-opts -agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,log_file_name=/local/$USER/sfs/sfs.log.yarn"
HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --ld-library-path $GRPC_HOME/libs/opt:$GRPC_HOME/third_party/protobuf/src/.lib"
SFS_STANDARD_OPTS="--sfs-logfilename /local/$USER/sfs/sfs.log --sfs-wrapped-scheme hdfs"
cp $SFS_DIRECTORY/sfs-adapter/target/sfs-adapter.jar $FLINK_HOME/lib/sfs-adapter.jar
cp $SFS_DIRECTORY/sfs-adapter/target/sfs-adapter.jar $HADOOP_HOME/share/hadoop/common/sfs-adapter.jar

if [ "$ENGINE" == "flink" ]; then
  srun $SRUN_STANDARD_OPTS ./start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS \
    --sfs-wrapped-fs "org.apache.flink.runtime.fs.hdfs.HadoopFileSystem"
else
  srun $SRUN_STANDARD_OPTS ./start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS \
    --sfs-wrapped-fs "org.apache.hadoop.hdfs.DistributedFileSystem"
fi

# wait until all datanodes are connected
CONNECTED_DATANODES=0
while [ $CONNECTED_DATANODES -lt ${#NODES[@]} ]; do
  CONNECTED_DATANODES=$(srun --nodelist=$MASTER --nodes=1-1 grep -E "processReport: from storage [[:alnum:]\-]+ node DatanodeRegistration" /local/$HDFS_LOCAL_LOG_DIR/namenode-$MASTER.log | wc -l)
  echo "$CONNECTED_DATANODES of ${#NODES[@]} DataNodes connected ..."
  sleep 1s
done
echo "$(date): Starting HDFS done"

echo "$(date): Configuring Flink"
TASK_SLOTS=16
JOBMANAGER_MEMORY=4096
TASKMANAGER_MEMORY=40960
cp $FLINK_HOME/conf/flink-conf.yaml.template $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: $(($TASK_SLOTS * $TASK_SLOTS * ${#NODES[@]} * 4))" $FLINK_HOME/conf/flink-conf.yaml
sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: $HADOOP_HOME/etc/hadoop" $FLINK_HOME/conf/flink-conf.yaml
cat >> $FLINK_HOME/conf/flink-conf.yaml << EOF
taskmanager.memory.off-heap: true
env.java.opts: -agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,log_file_name=/local/$USER/sfs/sfs.log.flink
EOF
echo "$(date): Configuring Flink done"

rm -rf $FLINK_HOME/log/*
rm -rf $HADOOP_HOME/log-*
rm -rf $HADOOP_HOME/logs/*
rm -rf $SFS_TARGET_DIRECTORY/*

echo "$(date): Generating TeraSort data on HDFS"
$HADOOP_HOME/bin/hadoop jar \
  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar teragen \
  -Dmapreduce.job.maps=$((${#NODES[@]} * ${TASK_SLOTS})) \
  10995116277 hdfs://$MASTER:8020/user/$USER/input
echo "$(date): Generating TeraSort data on HDFS done"

$HADOOP_HOME/bin/hadoop fs -mkdir -p hdfs://$MASTER:8020/user/$USER/output

if [ "$ENGINE" == "flink" ]; then
  $FLINK_HOME/bin/flink run \
    --jobmanager yarn-cluster \
    --yarncontainer ${#NODES[@]} \
    --yarnslots $TASK_SLOTS \
    --yarnjobManagerMemory $JOBMANAGER_MEMORY \
    --yarntaskManagerMemory $TASKMANAGER_MEMORY \
    --class eastcircle.terasort.FlinkTeraSort \
    --parallelism $((${#NODES[@]} * $TASK_SLOTS)) \
    $TERASORT_DIRECTORY/target/scala-2.10/terasort_2.10-0.0.1.jar \
    sfs://$MASTER:8020 /user/$USER/input /user/$USER/output $((${#NODES[@]} * $TASK_SLOTS))
else
  $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar terasort \
    -Dmapreduce.job.maps=$((${#NODES[@]} * ${TASK_SLOTS})) \
    -Dmapreduce.job.reduces=$((${#NODES[@]} * ${TASK_SLOTS})) \
    sfs://$MASTER:8020/user/$USER/input sfs://$MASTER:8020/user/$USER/output
fi

#mkdir $HOME/$SLURM_JOB_ID-output
#echo "$(date): Copying output data from HDFS"
#$HADOOP_HOME/bin/hadoop fs -copyToLocal hdfs://$MASTER:8020/user/$USER/output file://$SFS_DIRECTORY/terasort-$SLURM_JOB_ID-output
#echo "$(date): Copying output data from HDFS done"

#echo "$(date): Validating output"
#$HADOOP_HOME/bin/hadoop jar \
#  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar teravalidate \
#  hdfs://$MASTER:8020/user/$USER/output hdfs://$MASTER:8020/user/$USER/report
#$HADOOP_HOME/bin/hadoop fs -copyToLocal hdfs://$MASTER:8020/user/$USER/report file://$SFS_DIRECTORY/terasort-$SLURM_JOB_ID-output
#echo "$(date): Validating output done"

echo "$(date): Stopping HDFS"
cp ./stop-hdfs-slurm.sh $HADOOP_HOME/sbin
srun --nodelist=$MASTER --nodes=1-1 --chdir=$HADOOP_HOME/sbin ./stop-hdfs-slurm.sh --colocate-datanode-with-namenode
echo "$(date): Stopping HDFS done"

echo "$(date): Cleaning Java processes"
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "$(date): Cleaning Java processes done"

echo "$(date): Copying logs"
cat > copy-logs.sh << EOF
#!/bin/bash
cd /local/$USER/sfs
for file in \$(ls sfs.log*); do
  cp \$file $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname)-\$file
done
EOF
chmod +x copy-logs.sh
srun ./copy-logs.sh
rm copy-logs.sh
echo "$(date): Copying logs done"

echo "$(date): Cleaning local directories"
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/hdfs
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/sfs
echo "$(date): Cleaning local directories done"
