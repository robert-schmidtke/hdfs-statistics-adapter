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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import de.zib.sfs.analysis.io.SfsInputFormat;

public class SfsAnalysis {

    private static final String INPUT_PATH_KEY = "inputPath";
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
        DataSet<OperationStatistics> operationStatistics = env
                .createInput(new SfsInputFormat(inputPath, prefix, hosts,
                        slotsPerHost));

        // For each host/process/source/category combination, aggregate
        // statistics over the specified time bin.
        DataSet<OperationStatistics> aggregatedOperationStatistics = operationStatistics
                .groupBy("hostname")
                .reduceGroup(
                        new GroupReduceFunction<OperationStatistics, OperationStatistics>() {

                            private static final long serialVersionUID = -6279446327088687733L;

                            @Override
                            public void reduce(
                                    Iterable<OperationStatistics> values,
                                    final Collector<OperationStatistics> out)
                                    throws Exception {
                                // some state to keep during iteration
                                int pid = Integer.MIN_VALUE;
                                long startTime = Long.MIN_VALUE;
                                Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics> aggregates = new HashMap<>();

                                for (OperationStatistics value : values) {
                                    // get the current aggregate for this source
                                    // and category
                                    Tuple2<OperationSource, OperationCategory> aggregateKey = Tuple2
                                            .of(value.getSource(),
                                                    value.getCategory());
                                    OperationStatistics aggregate = aggregates
                                            .get(aggregateKey);

                                    // The PIDs may change. This indicates a
                                    // switch to a new file, which begins with a
                                    // new time.
                                    if (pid != value.getPid()) {
                                        pid = value.getPid();

                                        // if we have a current aggregate, emit
                                        // it (will only be null on the first
                                        // iteration for each source/category
                                        // and time bin)
                                        if (aggregate != null) {
                                            out.collect(aggregate);
                                        }

                                        // start new aggregation for this
                                        // source/category and time bin
                                        aggregate = value.clone();
                                        aggregates.put(aggregateKey, aggregate);

                                        startTime = value.getStartTime();
                                    } else {
                                        // same PID, but the time bin may be
                                        // full
                                        if (value.getStartTime() - startTime >= timeBinDuration) {
                                            // emit current aggregate and put
                                            // the value in the next time bin
                                            out.collect(aggregate);

                                            aggregate = value.clone();
                                            aggregates.put(aggregateKey,
                                                    aggregate);

                                            startTime = value.getStartTime();
                                        } else {
                                            // just aggregate the current
                                            // statistics
                                            aggregate.add(value);
                                        }
                                    }
                                }

                                // collect remaining aggregates
                                aggregates.forEach((key, v) -> out.collect(v));
                            }
                        });

        // print some String representation for testing
        aggregatedOperationStatistics.reduceGroup(
                new RichGroupReduceFunction<OperationStatistics, String>() {

                    private static final long serialVersionUID = 7109785107772492236L;

                    @Override
                    public void reduce(Iterable<OperationStatistics> values,
                            Collector<String> out) throws Exception {
                        for (OperationStatistics aggregator : values) {
                            String output = aggregator.getHostname() + ": "
                                    + aggregator.getStartTime() + "-"
                                    + aggregator.getEndTime() + ": "
                                    + aggregator.getSource().name() + ": ";
                            switch (aggregator.getCategory()) {
                            case READ:
                                output += ((ReadDataOperationStatistics) aggregator)
                                        .getData();
                                break;
                            case WRITE:
                                output += ((DataOperationStatistics) aggregator)
                                        .getData();
                                break;
                            case OTHER:
                                output += aggregator.getName() + " x "
                                        + aggregator.getCount();
                                break;
                            }
                            out.collect(output);
                        }
                        out.close();
                    }
                }).print();
    }
}
