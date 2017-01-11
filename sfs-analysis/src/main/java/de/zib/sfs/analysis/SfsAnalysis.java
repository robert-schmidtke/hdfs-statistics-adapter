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

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.UnsortedGrouping;
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

        // read all input files
        DataSet<OperationStatistics> operationStatistics = env
                .createInput(new SfsInputFormat(inputPath, prefix, hosts,
                        slotsPerHost));

        // group by host, source and category
        UnsortedGrouping<OperationStatistics> groupedOperationStatistics = operationStatistics
                .groupBy("hostname", "source", "category");
        SortedGrouping<OperationStatistics> sortedOperationStatistics = groupedOperationStatistics
                .sortGroup("startTime", Order.ASCENDING);

        // aggregate operation statistics per host, operation and time bin
        GroupReduceOperator<OperationStatistics, OperationStatistics> aggregatedOperationStatistics = sortedOperationStatistics
                .reduceGroup(new RichGroupReduceFunction<OperationStatistics, OperationStatistics>() {

                    private static final long serialVersionUID = 8061425400468716923L;

                    @Override
                    public void reduce(Iterable<OperationStatistics> values,
                            Collector<OperationStatistics> out)
                            throws Exception {
                        Map<String, OperationStatistics> aggregatedOperationStatistics = new HashMap<>();

                        // keep the time so we can create time bins
                        long lastTime = Long.MAX_VALUE;
                        for (final OperationStatistics operationStatistics : values) {
                            long currentTime = operationStatistics
                                    .getStartTime();

                            // get unique aggregator for current operation type
                            // and aggregate
                            String operationID = operationStatistics
                                    .getHostname()
                                    + ":"
                                    + operationStatistics.getClassName()
                                    + "."
                                    + operationStatistics.getName();
                            OperationStatistics aggregator = aggregatedOperationStatistics
                                    .computeIfAbsent(operationID, k -> {
                                        try {
                                            return operationStatistics.clone();
                                        } catch (CloneNotSupportedException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                            aggregator.add(operationStatistics);

                            // make sure lastTime is initialized in the first
                            // iteration
                            lastTime = Math.min(currentTime, lastTime);

                            // if the current time and the last checkpoint are
                            // more than the specified time per bin apart,
                            // collect the aggregated statistics
                            if (currentTime - lastTime >= timeBinDuration) {
                                lastTime = currentTime;

                                for (OperationStatistics a : aggregatedOperationStatistics
                                        .values()) {
                                    out.collect(a);
                                }

                                // reset aggregate operation statistics
                                aggregatedOperationStatistics.clear();
                            }
                        }

                        // no more inputs, collect remaining and close output as
                        // well
                        for (OperationStatistics a : aggregatedOperationStatistics
                                .values()) {
                            out.collect(a);
                        }
                        out.close();
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
