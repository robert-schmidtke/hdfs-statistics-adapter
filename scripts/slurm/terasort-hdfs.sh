#!/bin/bash

#SBATCH -J terasort-hdfs
#SBATCH --exclusive
#SBATCH --open-mode=append

usage() {
  echo "Usage: sbatch --nodes=<NODES> terasort-hdfs.sh"
  echo "  -e|--engine <flink|spark|hadoop> (default: not specified)"
  echo "  -n|--no-sfs (default: disabled)"
  echo "  -c|--collectl (default: disabled)"
  echo "  -d|--data <gigabytes> (default: 1024)"
  echo "     --hadoop-tasks <tasks> (default: automatic)"
}

echo "$(date): terasort-hdfs.sh $@"

while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -e|--engine)
      ENGINE="$2"
      shift
      ;;
    -n|--no-sfs)
      NO_SFS="true"
      ;;
    -c|--collectl)
      COLLECTL="true"
      ;;
    -d|--data)
      DATA_GB="$2"
      shift
      ;;
    --hadoop-tasks)
      TERAGEN_MAPPERS="$2"
      shift
      ;;
    *)
      echo "Invalid argument detected."
      usage
      exit 1
  esac
  shift
done

if [ -z "$NO_SFS" ]; then
  echo "Running with SFS enabled"
else
  echo "Running with SFS disabled"
fi

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

OUT_FMT=bb

DATA_GB=${DATA_GB:-1024}

export HOSTNAME=$(hostname)

export FLINK_HOME=/nfs/scratch/$USER/flink-1.3.2

export SPARK_HOME=/nfs/scratch/$USER/spark-2.2.0

NODES=(`scontrol show hostnames`)
export NODES
export MASTER=${NODES[0]}

echo "Nodes: ${NODES[@]}"

export HADOOP_VERSION=2.7.4
export HADOOP_HOME=/nfs/scratch/$USER/hadoop-${HADOOP_VERSION}
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HDFS_LOCAL_DIR=$USER/hdfs
export HDFS_LOCAL_LOG_DIR=$HDFS_LOCAL_DIR/log

export GRPC_HOME=/nfs/scratch/$USER/grpc-1.6.1

export SFS_DIRECTORY=/nfs/scratch/$USER/hdfs-statistics-adapter
export SFS_TARGET_DIRECTORY=/nfs/scratch/$USER/statistics-fs/logs

export TERASORT_DIRECTORY=/nfs/scratch/$USER/terasort

echo "$(date): Cleaning Java processes"
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "$(date): Cleaning Java processes done"

# Flink's temporary directories are set by YARN if using HDFS

echo "$(date): Cleaning local directories"
# need separate script because of wildcard
cat > rm-tmp.sh << EOF
#!/bin/bash
rm -rf /tmp/* > /dev/null 2>&1
EOF
chmod +x rm-tmp.sh
srun ./rm-tmp.sh
srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER
srun -N$SLURM_JOB_NUM_NODES rm -rf /local_ssd/$USER
srun -N$SLURM_JOB_NUM_NODES rm -rf /tmp/$USER
echo "$(date): Cleaning local directories done"

echo "$(date): Creating local folders"
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/hdfs
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local_ssd/$USER/sfs
srun -N$SLURM_JOB_NUM_NODES mkdir -p /tmp/$USER/sfs
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local/$USER/flink
srun -N$SLURM_JOB_NUM_NODES mkdir -p /local_ssd/$USER/collectl
echo "$(date): Creating local folders done"

if [ "$COLLECTL" = "true" ]; then
  echo "$(date): Starting collectl"
  start_collectl_script="${SLURM_JOB_ID}-start-collectl.sh"
  cat >> $start_collectl_script << EOF
#!/bin/bash
nohup collectl -P -f /local_ssd/$USER/collectl -s cCdDmMnNZ > /local_ssd/$USER/collectl/collectl.log 2>&1 &
echo \$! > /local_ssd/$USER/collectl/collectl.pid
EOF
  chmod +x $start_collectl_script
  srun -N$SLURM_JOB_NUM_NODES $start_collectl_script
  rm $start_collectl_script
  echo "$(date): Starting collectl done"
fi

if [ -z "$NO_SFS" ]; then
  echo "$(date): Starting transformer JVMs"
  start_transformer_jvm_script="${SLURM_JOB_ID}-start-transformer-jvm.sh"
  cat >> $start_transformer_jvm_script << EOF
#!/bin/bash
nohup java -cp $SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar de.zib.sfs.instrument.ClassTransformationService --port 4242 --timeout -1 --trace-mmap n --verbose n --instrumentation-skip o > /local_ssd/$USER/sfs/transformer.log 2>&1 &
echo \$! > /local_ssd/$USER/sfs/transformer.pid
EOF
  chmod +x $start_transformer_jvm_script
  srun -N$SLURM_JOB_NUM_NODES $start_transformer_jvm_script
  rm $start_transformer_jvm_script
  echo "$(date): Starting transformer JVMs done"
fi

rm -rf $FLINK_HOME/log
mkdir $FLINK_HOME/log
rm -rf $SFS_TARGET_DIRECTORY
mkdir $SFS_TARGET_DIRECTORY

echo "$(date): Starting HDFS"
rm -rf $HADOOP_HOME/log-*
rm -rf $HADOOP_HOME/logs
mkdir $HADOOP_HOME/logs
cp ./start-hdfs-slurm.sh $HADOOP_HOME/sbin

# 256M block size, replication factor of 1, 56G total node memory for YARN, put first datanode on namenode host
HDFS_BLOCKSIZE=$((256 * 1048576))
SRUN_STANDARD_OPTS="--nodelist=$MASTER --nodes=1-1 --chdir=$HADOOP_HOME/sbin"
HDFS_STANDARD_OPTS="--blocksize $HDFS_BLOCKSIZE --replication 1 --memory 57344 --cores 16 --io-buffer 1048576 --colocate-datanode-with-namenode"
LD_LIBRARY_PATH_EXT="$GRPC_HOME/libs/opt:$GRPC_HOME/third_party/protobuf/src/.lib"

if [ -z "$NO_SFS" ]; then
  # configure some additional options for SFS
  OPTS="-agentpath:$SFS_DIRECTORY/sfs-agent/target/libsfs.so=trans_jar=$SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar,trans_address=0.0.0.0:4242"
  OPTS="$OPTS,bin_duration=1000000000,cache_size=60,out_dir=/local_ssd/$USER/sfs,out_fmt=$OUT_FMT,trace_mmap=n,verbose=n,instr_skip=o,trace_fds=y,os_pool_size=1024,lq_lock_cache=1024,mp_lock_cache=1024"
  CLIENT_OPTS="$OPTS,dos_pool_size=1024,rdos_pool_size=65536,tq_pool_size=65536,key=client"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --hadoop-opts $OPTS,dos_pool_size=8192,rdos_pool_size=8192,tq_pool_size=4096,key=hdfs"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --map-opts $OPTS,dos_pool_size=262144,rdos_pool_size=262144,tq_pool_size=262144,key=map"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --reduce-opts $OPTS,dos_pool_size=262144,rdos_pool_size=262144,tq_pool_size=262144,key=reduce"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --yarn-opts $OPTS,dos_pool_size=4096,rdos_pool_size=65536,tq_pool_size=8192,key=yarn"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --mrappmaster-opts $OPTS,dos_pool_size=4096,rdos_pool_size=65536,tq_pool_size=8192,key=master"
  HDFS_STANDARD_OPTS="$HDFS_STANDARD_OPTS --ld-library-path $LD_LIBRARY_PATH_EXT"
  SFS_STANDARD_OPTS="--sfs-wrapped-scheme hdfs --sfs-instrumentation-skip o --sfs-output-directory /local_ssd/$USER/sfs --sfs-output-format $OUT_FMT --sfs-trace-fds true"
  cp $SFS_DIRECTORY/sfs-adapter/target/sfs-adapter.jar $FLINK_HOME/lib/sfs-adapter.jar
  cp $SFS_DIRECTORY/sfs-adapter/target/sfs-adapter.jar $HADOOP_HOME/share/hadoop/common/sfs-adapter.jar

  srun $SRUN_STANDARD_OPTS $HADOOP_HOME/sbin/start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS \
    --sfs-wrapped-fs "org.apache.hadoop.hdfs.DistributedFileSystem"
else
  # just start HDFS regularly
  srun $SRUN_STANDARD_OPTS $HADOOP_HOME/sbin/start-hdfs-slurm.sh $HDFS_STANDARD_OPTS $SFS_STANDARD_OPTS
fi

# get all configured datanodes
HADOOP_DATANODES=()
while IFS= read -r datanode; do HADOOP_DATANODES=(${HADOOP_DATANODES[@]} $datanode); done < $HADOOP_HOME/etc/hadoop/slaves

# wait until all datanodes are connected
CONNECTED_DATANODES=0
while [ $CONNECTED_DATANODES -lt ${#HADOOP_DATANODES[@]} ]; do
#   CONNECTED_DATANODES=$(srun --nodelist=$MASTER --nodes=1-1 grep -E "processReport( [[:alnum:]]+)?: from storage [[:alnum:]\-]+ node DatanodeRegistration" /local/$HDFS_LOCAL_LOG_DIR/namenode-$MASTER.log | wc -l)
  CONNECTED_DATANODES=$(srun --nodelist=$MASTER --nodes=1-1 grep -E "processReport( [[:alnum:]]+)?: from storage [[:alnum:]\-]+ node DatanodeRegistration" /local_ssd/$HDFS_LOCAL_LOG_DIR/namenode-$MASTER.log | wc -l)
  echo "$CONNECTED_DATANODES of ${#HADOOP_DATANODES[@]} DataNodes connected ..."
  sleep 1s
done
echo "$(date): Starting HDFS done"

TASK_SLOTS=16
JOBMANAGER_MEMORY=8192
TASKMANAGER_MEMORY=49152
case $ENGINE in
  flink)
    echo "$(date): Configuring Flink for TeraSort"
    cp $FLINK_HOME/conf/flink-conf.yaml.template $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^# taskmanager\.network\.numberOfBuffers/c\taskmanager.network.numberOfBuffers: $(($TASK_SLOTS * $TASK_SLOTS * ${#HADOOP_DATANODES[@]} * 4))" $FLINK_HOME/conf/flink-conf.yaml
    sed -i "/^# fs\.hdfs\.hadoopconf/c\fs.hdfs.hadoopconf: $HADOOP_HOME/etc/hadoop" $FLINK_HOME/conf/flink-conf.yaml
    cat >> $FLINK_HOME/conf/flink-conf.yaml << EOF
# blob.storage.directory: /local/$USER/flink
blob.storage.directory: /local_ssd/$USER/flink
taskmanager.memory.off-heap: true
taskmanager.memory.preallocate: true
akka.ask.timeout: 600 s
EOF

    if [ -z "$NO_SFS" ]; then
      cat >> $FLINK_HOME/conf/flink-conf.yaml << EOF
env.java.opts.jobmanager: $OPTS,dos_pool_size=32768,rdos_pool_size=65536,tq_pool_size=65536,key=flink
env.java.opts.taskmanager: $OPTS,dos_pool_size=262144,rdos_pool_size=262144,tq_pool_size=262144,key=flink
EOF
    fi
    echo "$(date): Configuring Flink for TeraSort done"
    ;;
  spark)
    echo "$(date): Configuring Spark for TeraSort"
    cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
    cat >> $SPARK_HOME/conf/spark-defaults.conf << EOF
spark.network.timeout 600s
EOF
    if [ -z "$NO_SFS" ]; then
      sed -i "/^# spark\.executor\.extraJavaOptions/c\spark.executor.extraJavaOptions $OPTS,dos_pool_size=262144,rdos_pool_size=262144,tq_pool_size=262144,key=spark" $SPARK_HOME/conf/spark-defaults.conf
      cat >> $SPARK_HOME/conf/spark-defaults.conf << EOF
spark.driver.extraJavaOptions $OPTS,dos_pool_size=262144,rdos_pool_size=262144,tq_pool_size=262144,key=spark
EOF
    fi
    echo "$(date): Configuring Spark for TeraSort done"
    ;;
  hadoop)
    # nothing to do
    ;;
esac

SCHEME="hdfs"
if [ -z "$NO_SFS" ]; then
  SCHEME="sfs"
fi

# total amount of data to generate, in bytes
# conveniently as multiple of gigabytes
# (well, almost a gigabyte, to ensure divisibility by 100)
TOTAL_DATA=$(($DATA_GB * 1073741800))

# figure out the number of mappers to use for generation of data
# rounding up one just in case
DATA_PER_MAPPER=$((512 * 1048576))
if [ -z "$TERAGEN_MAPPERS" ]; then
  TERAGEN_MAPPERS=$((($TOTAL_DATA + $DATA_PER_MAPPER - 1) / $DATA_PER_MAPPER))
fi

echo "$(date): Dumping file system counters"
ssh cumulus cat /sys/fs/xfs/sda1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.root.pre
ssh cumulus cat /sys/fs/xfs/sda2/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.local.pre
ssh cumulus cat /sys/fs/xfs/sdb1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.scratch.pre
dump_xfs_stats_script="${SLURM_JOB_ID}-dump_xfs_stats.sh"
cat > $dump_xfs_stats_script << EOF
#!/bin/bash
cat /sys/fs/xfs/sda2/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.root.pre
cat /sys/fs/xfs/sda3/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.tmp.pre
cat /sys/fs/ext4/sda5/session_write_kbytes > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).ext4.local_ssd.pre
cat /sys/fs/xfs/sdb1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.local.pre
EOF
chmod +x $dump_xfs_stats_script
srun -N$SLURM_JOB_NUM_NODES $dump_xfs_stats_script
rm $dump_xfs_stats_script
echo "$(date): Dumping file system counters done"

# set options for client as well
OLD_JAVA_OPTIONS="$_JAVA_OPTIONS"
export _JAVA_OPTIONS="$_JAVA_OPTIONS $CLIENT_OPTS"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$LD_LIBRARY_PATH_EXT"

echo "$(date): Generating TeraSort data on HDFS"
$HADOOP_HOME/bin/hadoop jar \
  $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar teragen \
  -Dmapreduce.job.maps=$TERAGEN_MAPPERS \
  $(($TOTAL_DATA / 100)) $SCHEME://$MASTER:8020/user/$USER/input
echo "$(date): Generating TeraSort data on HDFS done"

echo "$(date): Dumping file system counters"
ssh cumulus cat /sys/fs/xfs/sda1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.root.mid
ssh cumulus cat /sys/fs/xfs/sda2/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.local.mid
ssh cumulus cat /sys/fs/xfs/sdb1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.scratch.mid
dump_xfs_stats_script="${SLURM_JOB_ID}-dump_xfs_stats.sh"
cat > $dump_xfs_stats_script << EOF
#!/bin/bash
cat /sys/fs/xfs/sda2/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.root.mid
cat /sys/fs/xfs/sda3/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.tmp.mid
cat /sys/fs/ext4/sda5/session_write_kbytes > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).ext4.local_ssd.mid
cat /sys/fs/xfs/sdb1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.local.mid
EOF
chmod +x $dump_xfs_stats_script
srun -N$SLURM_JOB_NUM_NODES $dump_xfs_stats_script
rm $dump_xfs_stats_script
echo "$(date): Dumping file system counters done"

$HADOOP_HOME/bin/hadoop fs -mkdir -p hdfs://$MASTER:8020/user/$USER/output

echo "$(date): Running TeraSort"
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
      $SCHEME://$MASTER:8020 /user/$USER/input /user/$USER/output $((${#HADOOP_DATANODES[@]} * $TASK_SLOTS)) \
      100000 10
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
      $SCHEME://$MASTER:8020 /user/$USER/input /user/$USER/output $((${#HADOOP_DATANODES[@]} * $TASK_SLOTS)) \
      100000 10
    ;;
  hadoop)
    $HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-${HADOOP_VERSION}.jar terasort \
      -Dmapreduce.job.maps=$TERAGEN_MAPPERS \
      -Dmapreduce.job.reduces=$TERAGEN_MAPPERS \
      -Dmapreduce.terasort.partitions.sample=100000 \
      -Dmapreduce.terasort.num.partitions=10 \
      $SCHEME://$MASTER:8020/user/$USER/input $SCHEME://$MASTER:8020/user/$USER/output
    ;;
esac
RET_CODE=$?
echo "$(date): Running TeraSort done: $RET_CODE"

# restore _JAVA_OPTIONS
export _JAVA_OPTIONS="$OLD_JAVA_OPTIONS"

echo "$(date): Dumping file system counters"
ssh cumulus cat /sys/fs/xfs/sda1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.root.post
ssh cumulus cat /sys/fs/xfs/sda2/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.local.post
ssh cumulus cat /sys/fs/xfs/sdb1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-cumulus.xfs.scratch.post
dump_xfs_stats_script="${SLURM_JOB_ID}-dump_xfs_stats.sh"
cat > $dump_xfs_stats_script << EOF
#!/bin/bash
cat /sys/fs/xfs/sda2/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.root.post
cat /sys/fs/xfs/sda3/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.tmp.post
cat /sys/fs/ext4/sda5/session_write_kbytes > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).ext4.local_ssd.post
cat /sys/fs/xfs/sdb1/stats/stats > $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname).xfs.local.post
EOF
chmod +x $dump_xfs_stats_script
srun -N$SLURM_JOB_NUM_NODES $dump_xfs_stats_script
rm $dump_xfs_stats_script
echo "$(date): Dumping file system counters done"

# do not clean if job was not successful
if [ "$RET_CODE" -ne "0" ]; then
  NO_CLEAN="--no-clean"
fi
echo "$(date): Stopping HDFS $NO_CLEAN"
cp ./stop-hdfs-slurm.sh $HADOOP_HOME/sbin
srun $SRUN_STANDARD_OPTS $HADOOP_HOME/sbin/stop-hdfs-slurm.sh --colocate-datanode-with-namenode $NO_CLEAN
echo "$(date): Stopping HDFS done"

if [ -z "$NO_SFS" ]; then
  echo "$(date): Stopping transformer JVMs"
  stop_transformer_jvm_script="${SLURM_JOB_ID}-stop-transformer-jvm.sh"
  cat >> $stop_transformer_jvm_script << EOF
#!/bin/bash
kill \$(</local_ssd/$USER/sfs/transformer.pid)
rm /local_ssd/$USER/sfs/transformer.pid
echo "Transformer log on \$(hostname):"
cat /local_ssd/$USER/sfs/transformer.log
rm /local_ssd/$USER/sfs/transformer.log
EOF
  chmod +x $stop_transformer_jvm_script
  srun -N$SLURM_JOB_NUM_NODES $stop_transformer_jvm_script
  rm $stop_transformer_jvm_script
  echo "$(date): Stopping transformer JVMs done"
fi

if [ "$COLLECTL" = "true" ]; then
  mkdir -p $SFS_DIRECTORY/$SLURM_JOB_ID-$ENGINE-terasort-collectl
  echo "$(date): Stopping collectl"
  stop_collectl_script="${SLURM_JOB_ID}-stop-collectl.sh"
  cat >> $stop_collectl_script << EOF
#!/bin/bash
pid=\$(</local_ssd/$USER/collectl/collectl.pid)
kill \$pid
while [ -e /proc/\$pid ]; do sleep 1s; done

rm /local_ssd/$USER/collectl/collectl.pid
rm /local_ssd/$USER/collectl/collectl.log
cp /local_ssd/$USER/collectl/*.gz $SFS_DIRECTORY/$SLURM_JOB_ID-$ENGINE-terasort-collectl
EOF
  chmod +x $stop_collectl_script
  srun $stop_collectl_script
  rm $stop_collectl_script
  echo "$(date): Stopping collectl done"
fi

echo "$(date): Cleaning Java processes"
srun -N$SLURM_JOB_NUM_NODES killall -sSIGKILL java
echo "$(date): Cleaning Java processes done"

if [ -z "$NO_SFS" ]; then
  echo "$(date): Copying logs"
  cat > copy-logs.sh << EOF
#!/bin/bash

echo "Size of files in /local_ssd/$USER/sfs on \$(hostname):"
du -c -h /local_ssd/$USER/sfs

# execute postrun aggregation for CSVs
if [ "$OUT_FMT" == "csv" ]; then
  java -cp $SFS_DIRECTORY/sfs-agent/target/sfs-agent.jar de.zib.sfs.instrument.statistics.PostRunOperationStatisticsAggregator --path /local_ssd/$USER/sfs --prefix "\$(hostname)-" --suffix "-concat" --delete
fi

cd /local_ssd/$USER/sfs
for file in \$(find . -name "*.$OUT_FMT"); do
  # this includes SFS and JVM logs, as well as the file descriptor mappings
  cp \$file $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(basename \$file)
done
cd /local_ssd/$USER/
for file in \$(find . -name "*-metrics.out"); do
  cp \$file $SFS_TARGET_DIRECTORY/$SLURM_JOB_ID-\$(hostname)-\$(basename \$file)
done
EOF
  chmod +x copy-logs.sh
  srun ./copy-logs.sh
  rm copy-logs.sh
  echo "$(date): Copying logs done"
fi

# put the files in a separate, uncompressed archive for BBs
if [ "$OUT_FMT" == "bb" ]; then
  cd $SFS_TARGET_DIRECTORY
  ls -1A *.$OUT_FMT > $OUT_FMT.files
  tar cf $OUT_FMT.tar --files-from $OUT_FMT.files
  rm $OUT_FMT.files
  find . -name "*.$OUT_FMT" | xargs rm
fi

# pack the results
tar czf $SFS_DIRECTORY/$SLURM_JOB_ID-$ENGINE-terasort-results.tar.gz $SFS_TARGET_DIRECTORY

# need separate script because of wildcard
cd $SFS_DIRECTORY/scripts/slurm
srun ./rm-tmp.sh
rm rm-tmp.sh

if [ "$RET_CODE" -eq "0" ]; then
  echo "$(date): Cleaning local directories"
  srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER
  srun -N$SLURM_JOB_NUM_NODES rm -rf /local_ssd/$USER
  srun -N$SLURM_JOB_NUM_NODES rm -rf /tmp/$USER
  srun -N$SLURM_JOB_NUM_NODES rm -rf /local/$USER/collectl
  echo "$(date): Cleaning local directories done"
else
  echo "$(date): Some task did not run successfully, not cleaning local directories."
fi

echo "$(date): Done."
