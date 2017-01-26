#!/bin/bash

#SBATCH -J terasort-hdfs
#SBATCH --exclusive
#SBATCH --open-mode=append

usage() {
  echo "Usage: sbatch --nodes=<NODES> terasort-hdfs.sh"
  echo "  -e|--engine <flink|spark|hadoop> (default: not specified)"
  echo "  -n|--no-sfs (default: disabled)"
}

while [[ $# -gt 1 ]]; do
  key="$1"
  case $key in
    -e|--engine)
      ENGINE="$2"
      shift
      ;;
    -n|--no-sfs)
      NO_SFS="true"
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
    flink|spark|hadoop)
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

export SPARK_HOME=/scratch/$USER/spark-2.1.0

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
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/flink
srun -N$SLURM_JOB_NUM_NODES rm -rf /tmp/*
echo "$(date): Cleaning local directories done"

echo "$(date): Creating local folders"
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/hdfs
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/sfs
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/flink
echo "$(date): Creating local folders done"

if [ -z "$NO_SFS" ]; then
  echo "$(date): Starting transformer JVMs"
  start_transformer_jvm_script="${SLURM_JOB_ID}-start-transformer-jvm.sh"
  cat >> $start_transformer_jvm_script << EOF
#!/bin/bash
nohup java -cp $SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar de.zib.sfs.instrument.ClassTransformationService --port 4242 --timeout -1 --verbose n > /local/$USER/sfs/transformer.log 2>&1 &
echo \$! > /local/$USER/sfs/transformer.pid
EOF
  chmod +x $start_transformer_jvm_script
  srun -N$SLURM_JOB_NUM_NODES $start_transformer_jvm_script
  rm $start_transformer_jvm_script
  echo "$(date): Starting transformer JVMs done"
fi

echo "$(date): Starting HDFS"
rm -rf $HADOOP_HOME/logs/*
cp ./start-hdfs-slurm.sh $HADOOP_HOME/sbin

# 256M block size, replication factor of 1, 50G total node memory for YARN, put first datanode on namenode host
SRUN_STANDARD_OPTS="--nodelist=$MASTER --nodes=1-1 --chdir=$HADOOP_HOME/sbin"
HDFS_STANDARD_OPTS="--blocksize 268435456 --replication 1 --memory 51200 --cores 16 --io-buffer 1048576 --colocate-datanode-with-namenode"
if [ -z "$NO_SFS" ]; then
  # configure some additional options for SFS
  OPTS="-agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,trans_address=0.0.0.0:4242,verbose=n"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --hadoop-opts $OPTS,log_file_name=/local/$USER/sfs/sfs.log.hadoop,key=hadoop"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --map-opts $OPTS,log_file_name=/local/$USER/sfs/sfs.log.map,key=map"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --reduce-opts $OPTS,log_file_name=/local/$USER/sfs/sfs.log.reduce,key=reduce"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --yarn-opts $OPTS,log_file_name=/local/$USER/sfs/sfs.log.yarn,key=yarn"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --ld-library-path $GRPC_HOME/libs/opt:$GRPC_HOME/third_party/protobuf/src/.lib"
  SFS_STANDARD_OPTS="--sfs-logfilename /local/$USER/sfs/sfs.log --sfs-wrapped-scheme hdfs"
  cp $SFS_DIRECTORY/sfs-adapter/target/sfs-adapter.jar $FLINK_HOME/lib/sfs-adapter.jar
  cp $SFS_DIRECTORY/sfs-adapter/target/sfs-adapter.jar $HADOOP_HOME/share/hadoop/common/sfs-adapter.jar

  case $ENGINE in
    flink)
      srun $SRUN_STANDARD_OPTS ./start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS \
        --sfs-wrapped-fs "org.apache.flink.runtime.fs.hdfs.HadoopFileSystem"
      ;;
    spark)
      # Spark does not have a wrapper for HDFS
      srun $SRUN_STANDARD_OPTS ./start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS \
        --sfs-wrapped-fs "org.apache.hadoop.hdfs.DistributedFileSystem"
      ;;
    hadoop)
      srun $SRUN_STANDARD_OPTS ./start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS \
        --sfs-wrapped-fs "org.apache.hadoop.hdfs.DistributedFileSystem"
      ;;
  esac
else
  # just start HDFS regularly
  srun $SRUN_STANDARD_OPTS ./start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS
fi

# get all configured datanodes
HADOOP_DATANODES=()
while IFS= read -r datanode; do HADOOP_DATANODES=(${HADOOP_DATANODES[@]} $datanode); done < $HADOOP_HOME/etc/hadoop/slaves

# wait until all datanodes are connected
CONNECTED_DATANODES=0
while [ $CONNECTED_DATANODES -lt ${#HADOOP_DATANODES[@]} ]; do
  CONNECTED_DATANODES=$(srun --nodelist=$MASTER --nodes=1-1 grep -E "processReport: from storage [[:alnum:]\-]+ node DatanodeRegistration" /local/$HDFS_LOCAL_LOG_DIR/namenode-$MASTER.log | wc -l)
  echo "$CONNECTED_DATANODES of ${#HADOOP_DATANODES[@]} DataNodes connected ..."
  sleep 1s
done
echo "$(date): Starting HDFS done"

TASK_SLOTS=16
JOBMANAGER_MEMORY=4096
TASKMANAGER_MEMORY=40960
case $ENGINE in
  flink)
    echo "$(date): Configuring Flink for TeraSort"
    cp $FLINK_HOME/conf/flink-conf.yaml.template $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: $(($TASK_SLOTS * $TASK_SLOTS * ${#HADOOP_DATANODES[@]} * 4))" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: $HADOOP_HOME/etc/hadoop" $FLINK_HOME/conf/flink-conf.yaml
    cat >> $FLINK_HOME/conf/flink-conf.yaml << EOF
blob.storage.directory: /local/$USER/flink
taskmanager.memory.off-heap: true
env.java.opts: $OPTS,log_file_name=/local/$USER/sfs/sfs.log.flink,key=flink
EOF
    echo "$(date): Configuring Flink for TeraSort done"
    ;;
  spark)
    echo "$(date): Configuring Spark for TeraSort"
    cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
    sed -i "/^# spark\.executor\.extraJavaOptions/c\spark.executor.extraJavaOptions $OPTS,log_file_name=/local/$USER/sfs/sfs.log.spark.executor,key=executor" $SPARK_HOME/conf/spark-defaults.conf
    cat >> $SPARK_HOME/conf/spark-defaults.conf << EOF
spark.driver.extraJavaOptions $OPTS,log_file_name=/local/$USER/sfs/sfs.log.spark.driver,key=driver
EOF
    echo "$(date): Configuring Spark for TeraSort done"
    ;;
  hadoop)
    # nothing to do
    ;;
esac

rm -rf $FLINK_HOME/log/*
rm -rf $HADOOP_HOME/log-*
rm -rf $HADOOP_HOME/logs/*
rm -rf $SFS_TARGET_DIRECTORY/*

echo "$(date): Generating TeraSort data on HDFS"
$HADOOP_HOME/bin/hadoop jar \
  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar teragen \
  -Dmapreduce.job.maps=$((${#HADOOP_DATANODES[@]} * ${TASK_SLOTS})) \
  10995116277 hdfs://$MASTER:8020/user/$USER/input
echo "$(date): Generating TeraSort data on HDFS done"

$HADOOP_HOME/bin/hadoop fs -mkdir -p hdfs://$MASTER:8020/user/$USER/output

echo "$(date): Running TeraSort"
SCHEME="hdfs"
if [ -z "$NO_SFS" ];
  SCHEME="sfs"
fi

case $ENGINE in
  flink)
    $FLINK_HOME/bin/flink run \
      --jobmanager yarn-cluster \
      --yarncontainer ${#HADOOP_DATANODES[@]} \
      --yarnslots $TASK_SLOTS \
      --yarnjobManagerMemory $JOBMANAGER_MEMORY \
      --yarntaskManagerMemory $TASKMANAGER_MEMORY \
      --class eastcircle.terasort.FlinkTeraSort \
      --parallelism $((${#HADOOP_DATANODES[@]} * $TASK_SLOTS)) \
      $TERASORT_DIRECTORY/target/scala-2.10/terasort_2.10-0.0.1.jar \
      $SCHEME://$MASTER:8020 /user/$USER/input /user/$USER/output $((${#HADOOP_DATANODES[@]} * $TASK_SLOTS))
    ;;
  spark)
    $SPARK_HOME/bin/spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors ${#HADOOP_DATANODES[@]} \
      --executor-cores $TASK_SLOTS \
      --driver-memory "${JOBMANAGER_MEMORY}M" \
      --executor-memory "${TASKMANAGER_MEMORY}M" \
      --class eastcircle.terasort.SparkTeraSort \
      $TERASORT_DIRECTORY/target/scala-2.10/terasort_2.10-0.0.1.jar \
      $SCHEME://$MASTER:8020 /user/$USER/input /user/$USER/output $((${#HADOOP_DATANODES[@]} * $TASK_SLOTS))
    ;;
  hadoop)
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar terasort \
      -Dmapreduce.job.maps=$((${#HADOOP_DATANODES[@]} * ${TASK_SLOTS})) \
      -Dmapreduce.job.reduces=$((${#HADOOP_DATANODES[@]} * ${TASK_SLOTS})) \
      $SCHEME://$MASTER:8020/user/$USER/input $SCHEME://$MASTER:8020/user/$USER/output
    ;;
esac
RET_CODE=$?
echo "$(date): Running TeraSort done: $RET_CODE"

echo "$(date): Stopping HDFS"
cp ./stop-hdfs-slurm.sh $HADOOP_HOME/sbin
srun --nodelist=$MASTER --nodes=1-1 --chdir=$HADOOP_HOME/sbin ./stop-hdfs-slurm.sh --colocate-datanode-with-namenode
echo "$(date): Stopping HDFS done"

if [ -z "$NO_SFS" ]; then
  echo "$(date): Stopping transformer JVMs"
  stop_transformer_jvm_script="${SLURM_JOB_ID}-stop-transformer-jvm.sh"
  cat >> $stop_transformer_jvm_script << EOF
#!/bin/bash
kill \$(</local/$USER/sfs/transformer.pid)
rm /local/$USER/sfs/transformer.pid
echo "Transformer log on \$(hostname):"
cat /local/$USER/sfs/transformer.log
rm /local/$USER/sfs/transformer.log
EOF
  chmod +x $stop_transformer_jvm_script
  srun -N$SLURM_JOB_NUM_NODES $stop_transformer_jvm_script
  rm $stop_transformer_jvm_script
  echo "$(date): Stopping transformer JVMs done"
fi

if [ -z "$NO_SFS" ]; then
  if [ "$RET_CODE" -eq "0" ]; then
    echo "$(date): Configuring Flink for Analysis"
    cp $FLINK_HOME/conf/flink-conf.yaml.template $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: $MASTER" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^jobmanager\.heap\.mb/c\jobmanager.heap.mb: $JOBMANAGER_MEMORY" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^taskmanager\.heap\.mb/c\taskmanager.heap.mb: $TASKMANAGER_MEMORY" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^taskmanager\.numberOfTaskSlots/c\taskmanager.numberOfTaskSlots: $TASK_SLOTS" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: $(($TASK_SLOTS * $TASK_SLOTS * ${#NODES[@]} * 4))" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^# taskmanager\.tmp\.dirs/c\taskmanager.tmp.dirs: /local/$USER/flink" $FLINK_HOME/conf/flink-conf.yaml
    printf "%s\n" "${NODES[@]}" > $FLINK_HOME/conf/slaves
    cat >> $FLINK_HOME/conf/flink-conf.yaml << EOF
blob.storage.directory: /local/$USER/flink
taskmanager.memory.off-heap: true
EOF
    echo "$(date): Configuring Flink for Analysis done"

    echo "$(date): Starting Flink cluster"
    cp ./start-flink-cluster-slurm.sh $FLINK_HOME/bin
    srun --nodelist="$MASTER" --nodes=1-1 --chdir=$FLINK_HOME/bin ./start-flink-cluster-slurm.sh

    # wait until all taskmanagers are connected
    CONNECTED_TASKMANAGERS=0
    while [ $CONNECTED_TASKMANAGERS -lt ${#NODES[@]} ]; do
      CONNECTED_TASKMANAGERS=$(grep -R "Registered TaskManager" $FLINK_HOME/log/flink-$USER-jobmanager-*-$MASTER.log | wc -l)
      echo "$CONNECTED_TASKMANAGERS of ${#NODES[@]} TaskManagers connected ..."
      sleep 1s
    done
    echo "$(date): Starting Flink cluster done"

    echo "$(date): Running Analysis"
    HOSTS=$(printf ",%s" "${NODES[@]}")
    HOSTS=${HOSTS:1}
    $FLINK_HOME/bin/flink run \
      --class de.zib.sfs.analysis.SfsAnalysis \
      --parallelism $((${#NODES[@]} * $TASK_SLOTS)) \
      $SFS_DIRECTORY/sfs-analysis/target/sfs-analysis-1.0-SNAPSHOT.jar \
      --inputPath /local/$USER/sfs \
      --outputPath $SFS_TARGET_DIRECTORY \
      --prefix "sfs.log" \
      --hosts $HOSTS \
      --slotsPerHost $TASK_SLOTS \
      --timeBinDuration 1000 \
      --timeBinCacheSize 30
#      --printExecutionPlanOnly true
    RET_CODE=$?
    echo "$(date): Running Analysis done"

    echo "$(date): Stopping Flink cluster"
    cp ./stop-flink-cluster-slurm.sh $FLINK_HOME/bin
    srun --nodelist="$MASTER" --nodes=1-1 --chdir=$FLINK_HOME/bin ./stop-flink-cluster-slurm.sh
    echo "$(date): Waiting for Flink cluster to be stopped"
    sleep 10s
    echo "$(date): Stopping Flink cluster done"
  else
    echo "$(date): Main Job did not run successfully, skipping analysis."
  fi
fi

echo "$(date): Cleaning Java processes"
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "$(date): Cleaning Java processes done"

if [ -z "$NO_SFS" ]; then
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
fi

if [ "$RET_CODE" -eq "0" ]; then
  echo "$(date): Cleaning local directories"
  srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/hdfs
  srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/sfs
  srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/flink
  srun -N$SLURM_JOB_NUM_NODES rm -rf /tmp/*
  echo "$(date): Cleaning local directories done"
else
  echo "$(date): Some task did not run successfully, not cleaning local directories."
fi

echo "$(date): Done."
