/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.io.SfsInputFormat;
import de.zib.sfs.analysis.io.SfsOutputFormat;

public class SfsAnalysis {

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsAnalysis.class);

    private static final String INPUT_PATH_KEY = "inputPath";
    private static final String OUTPUT_PATH_KEY = "outputPath";

    private static final String PREFIX_KEY = "prefix";

    private static final String HOSTS_KEY = "hosts";

    private static final String SLOTS_PER_HOST_KEY = "slotsPerHost";

    private static final String TIME_BIN_DURATION_KEY = "timeBinDuration";

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
        try {
            hosts = params.get(HOSTS_KEY).split(",");
            slotsPerHost = params.getInt(SLOTS_PER_HOST_KEY, 1);
            timeBinDuration = params.getLong(TIME_BIN_DURATION_KEY, 1000);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        final ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

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

        // For each host/source/class/operation combination, aggregate
        // statistics over the specified time bin, then repeat for
        // host/class/operation (this reduces parallelism by a
        // factor of slotsPerHost).
        OperationStatisticsGroupReducer reducer = new OperationStatisticsGroupReducer(
                timeBinDuration);
        DataSet<OperationStatistics> aggregatedOperationStatistics = operationStatistics
                .groupBy("hostname", "internalId", "className", "name")
                .reduceGroup(reducer).groupBy("hostname", "className", "name")
                .reduceGroup(reducer);

        // for each host/source/category combination, sort the aggregated
        // statistics records in ascending time
        DataSet<OperationStatistics> sortedAggregatedOperationStatistics = aggregatedOperationStatistics
                .groupBy("hostname", "source", "category")
                .sortGroup("startTime", Order.ASCENDING)
                .reduceGroup(
                        new GroupReduceFunction<OperationStatistics, OperationStatistics>() {

                            private static final long serialVersionUID = 2289217231165874999L;

                            @Override
                            public void reduce(
                                    Iterable<OperationStatistics> values,
                                    Collector<OperationStatistics> out)
                                    throws Exception {
                                values.forEach(v -> out.collect(v));
                            }
                        });

        // write the output (one file per host, source and category)
        sortedAggregatedOperationStatistics.output(new SfsOutputFormat(
                outputPath, ",", hosts, slotsPerHost));

        // currently printing the execution plan and executing the program are
        // mutually exclusive
        if (LOG.isDebugEnabled()) {
            LOG.debug("ExecutionPlan: {}", env.getExecutionPlan());
        } else {
            // now run the entire thing
            env.execute(SfsAnalysis.class.getName());
        }
    }
}
