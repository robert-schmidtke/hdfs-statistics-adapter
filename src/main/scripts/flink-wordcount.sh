#!/bin/bash

#PBS -N flink-wordcount
#PBS -j oe

source /opt/modules/default/init/bash
module load ccm
module unload atp # abnormal termination processing
cd $PBS_O_WORKDIR

cat > launch-$PBS_JOBID.sh << EOF
#!/bin/bash

module load java/jdk1.8.0_51

echo "\$(hostname): \$(date)"

export HADOOP_PREFIX=$WORK/statistics-fs/hadoop-2.7.2
export HADOOP_HOME=\$HADOOP_PREFIX
export FLINK_HOME=$WORK/statistics-fs/flink-1.1.1

IFS=$'\n' read -d '' -r -a NODES < $PBS_NODEFILE
MASTER=(\$(hostname))
SLAVES=(\${NODES[@]/\$MASTER})
MASTER=\${MASTER[0]}

echo "Master: \$MASTER, Slaves: \${SLAVES[@]}"

TASK_SLOTS=10

sed -i "/^jobmanager\.rpc\.address/c\jobmanager.rpc.address: \$MASTER" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^jobmanager\.heap\.mb/c\jobmanager.heap.mb: 4096" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^taskmanager\.heap\.mb/c\taskmanager.heap.mb: 102400" \$FLINK_HOME/conf/flink-conf.yaml
sed -i "/^taskmanager\.numberOfTaskSlots/c\taskmanager.numberOfTaskSlots: \$TASK_SLOTS" \$FLINK_HOME/conf/flink-conf.yaml

printf "%s\n" "\${SLAVES[@]}" > \$FLINK_HOME/conf/slaves

\$FLINK_HOME/bin/start-cluster.sh

CONNECTED_TASKMANAGERS=0
while [ \$CONNECTED_TASKMANAGERS -lt \${#SLAVES[@]} ]; do
  CONNECTED_TASKMANAGERS=\$(grep -R "Registered TaskManager" \$FLINK_HOME/log/flink-$USER-jobmanager-0-\$MASTER.log | wc -l)
  echo "\$CONNECTED_TASKMANAGERS of \${#SLAVES[@]} TaskManagers connected ..."
  sleep 1s
done

rm -rf $WORK/statistics-fs/output/pg1661.txt
\$FLINK_HOME/bin/flink run \
  -c org.apache.flink.examples.java.wordcount.WordCount \
  -p \$((\${#SLAVES[@]} * \$TASK_SLOTS)) \
  \$FLINK_HOME/examples/batch/WordCount.jar \
  --input $WORK/statistics-fs/input/pg1661.txt \
  --output $WORK/statistics-fs/output/pg1661.txt

\$FLINK_HOME/bin/stop-cluster.sh

# this makes the scheduler not kill the JobManager's JVM before it is completely shut down
sleep 5s

EOF

chmod +x launch-$PBS_JOBID.sh
ccmrun ./launch-$PBS_JOBID.sh
rm ./launch-$PBS_JOBID.sh
