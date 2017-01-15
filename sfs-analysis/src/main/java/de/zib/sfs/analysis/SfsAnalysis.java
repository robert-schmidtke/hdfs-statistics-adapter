/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.io.SfsInputFormat;
import de.zib.sfs.analysis.io.SfsOutputFormat;
import de.zib.sfs.analysis.statistics.OperationCategory;
import de.zib.sfs.analysis.statistics.OperationSource;
import de.zib.sfs.analysis.statistics.OperationStatistics;

public class SfsAnalysis {

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsAnalysis.class);

    private static final String INPUT_PATH_KEY = "inputPath";
    private static final String OUTPUT_PATH_KEY = "outputPath";

    private static final String PREFIX_KEY = "prefix";

    private static final String HOSTS_KEY = "hosts";

    private static final String SLOTS_PER_HOST_KEY = "slotsPerHost";

    private static final String TIME_BIN_DURATION_KEY = "timeBinDuration";

    private static final String TIME_BIN_CACHE_SIZE_KEY = "timeBinCacheSize";

    private static final String PRINT_EXECUTION_PLAN_ONLY_KEY = "printExecutionPlanOnly";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String inputPath = params.get(INPUT_PATH_KEY);
        if (inputPath == null || inputPath.isEmpty()) {
            throw new IllegalArgumentException(INPUT_PATH_KEY
                    + " cannot be empty");
        }

        String outputPath = params.get(OUTPUT_PATH_KEY);
        if (outputPath == null || outputPath.isEmpty()) {
            throw new IllegalArgumentException(OUTPUT_PATH_KEY
                    + " cannot be empty");
        }

        String prefix = params.get(PREFIX_KEY);

        String[] hosts;
        int slotsPerHost;
        final long timeBinDuration;
        final int timeBinCacheSize;
        final boolean printExecutionPlanOnly;
        try {
            hosts = params.get(HOSTS_KEY).split(",");
            slotsPerHost = params.getInt(SLOTS_PER_HOST_KEY, 1);
            timeBinDuration = params.getLong(TIME_BIN_DURATION_KEY, 1000);
            timeBinCacheSize = params.getInt(TIME_BIN_CACHE_SIZE_KEY, 8);
            printExecutionPlanOnly = params.getBoolean(
                    PRINT_EXECUTION_PLAN_ONLY_KEY, false);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        Map<Tuple3<String, OperationSource, OperationCategory>, Integer> partitionsLookup = new HashMap<>();
        int partition = 0;
        for (String host : hosts) {
            for (OperationSource source : OperationSource.values()) {
                for (OperationCategory category : OperationCategory.values()) {
                    partitionsLookup.put(Tuple3.of(host, source, category),
                            partition++);
                }
            }
        }

        // Read all input files, each split containing zero or more files, one
        // file per process and per host. Each file contains chronologically
        // ordered log lines for I/O operations, one operation per line.
        DataSource<OperationStatistics> operationStatistics = env
                .createInput(new SfsInputFormat(inputPath, prefix, hosts,
                        slotsPerHost));

        // make the grouping and partitioning information available to Flink
        operationStatistics.getSplitDataProperties().splitsGroupedBy(
                "hostname;internalId");
        operationStatistics.getSplitDataProperties().splitsPartitionedBy(
                "hostname;internalId");

        // For each host/source combination, aggregate statistics over the
        // specified time bin.
        DataSet<OperationStatistics.Aggregator> aggregatedOperationStatistics = operationStatistics
                .groupBy("hostname", "internalId")
                .reduceGroup(
                        new OperationStatisticsAggregator(timeBinDuration,
                                timeBinCacheSize))
                .withForwardedFields("hostname->hostname")
                .setParallelism(hosts.length * slotsPerHost);

        // for each host/source/category combination, sort the aggregated
        // statistics records in ascending time
        DataSet<OperationStatistics.Aggregator> sortedAggregatedOperationStatistics = aggregatedOperationStatistics
                .groupBy("customKey")
                .withPartitioner(new Partitioner<String>() {

                    private static final long serialVersionUID = 2469900057020811866L;

                    @Override
                    public int partition(String key, int numPartitions) {
                        String[] splitKey = key.split(":");
                        return partitionsLookup.get(Tuple3.of(splitKey[0],
                                OperationSource.valueOf(splitKey[1]),
                                OperationCategory.valueOf(splitKey[2])));
                    }
                })
                .sortGroup("startTime", Order.ASCENDING)
                .reduceGroup(
                        new AggregatedOperationStatisticsAggregator(
                                timeBinDuration, timeBinCacheSize))
                .withForwardedFields("hostname->hostname")
                .setParallelism(
                        hosts.length * OperationCategory.values().length
                                * OperationSource.values().length);

        // write the output (one file per host, source and category)
        sortedAggregatedOperationStatistics.output(new SfsOutputFormat(
                outputPath, ",", hosts, slotsPerHost));

        // currently printing the execution plan and executing the program are
        // mutually exclusive
        if (printExecutionPlanOnly) {
            LOG.info("ExecutionPlan: {}", env.getExecutionPlan());
        } else {
            // now run the entire thing
            env.execute(SfsAnalysis.class.getName());
        }
    }
}
