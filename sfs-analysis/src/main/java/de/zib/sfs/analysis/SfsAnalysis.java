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
        DataSet<OperationInfo> operationInfos = env
                .createInput(new SfsInputFormat(inputPath, prefix, hosts,
                        slotsPerHost));

        // aggregate operation information per host, operation and time bin
        GroupReduceOperator<OperationInfo, OperationInfo.Aggregator> aggregatedOperationInfos = operationInfos
                .reduceGroup(new RichGroupReduceFunction<OperationInfo, OperationInfo.Aggregator>() {

                    private static final long serialVersionUID = 8061425400468716923L;

                    @Override
                    public void reduce(Iterable<OperationInfo> values,
                            Collector<OperationInfo.Aggregator> out)
                            throws Exception {
                        Map<String, OperationInfo.Aggregator> aggregatedOperationInfos = new HashMap<>();

                        // keep the time so we can create time bins
                        long lastTime = Long.MAX_VALUE;
                        for (final OperationInfo operationInfo : values) {
                            long currentTime = operationInfo.getStartTime();

                            // get unique aggregator for current
                            // operation type
                            String operationID = operationInfo.getHostname()
                                    + ":" + operationInfo.getClassName() + "."
                                    + operationInfo.getName();
                            OperationInfo.Aggregator aggregator = aggregatedOperationInfos
                                    .computeIfAbsent(operationID,
                                            k -> operationInfo.getAggregator());

                            // collect information
                            aggregator.aggregate(operationInfo);

                            // make sure lastTime is initialized in the
                            // first iteration
                            lastTime = Math.min(currentTime, lastTime);

                            // if the current time and the last checkpoint are
                            // more than the specified time per bin apart,
                            // collect the aggregated information
                            if (currentTime - lastTime >= timeBinDuration) {
                                lastTime = currentTime;

                                for (OperationInfo.Aggregator a : aggregatedOperationInfos
                                        .values()) {
                                    out.collect(a);
                                }

                                // reset aggregate operation information
                                aggregatedOperationInfos.clear();
                            }
                        }

                        // no more inputs, collect remaining and close output as
                        // well
                        for (OperationInfo.Aggregator a : aggregatedOperationInfos
                                .values()) {
                            out.collect(a);
                        }
                        out.close();
                    }
                });

        // group by host, source and category
        UnsortedGrouping<OperationInfo.Aggregator> groupedOperationInfos = aggregatedOperationInfos
                .groupBy("getHostname()", "getSource()", "getCategory()");
        SortedGrouping<OperationInfo.Aggregator> sortedOperationInfos = groupedOperationInfos
                .sortGroup("getMinStartTime()", Order.ASCENDING);

        // print some String representation for testing
        sortedOperationInfos
                .reduceGroup(
                        new RichGroupReduceFunction<OperationInfo.Aggregator, String>() {

                            private static final long serialVersionUID = 7109785107772492236L;

                            @Override
                            public void reduce(
                                    Iterable<OperationInfo.Aggregator> values,
                                    Collector<String> out) throws Exception {
                                for (OperationInfo.Aggregator aggregator : values) {
                                    String output = aggregator.getHostname()
                                            + ": "
                                            + aggregator.getMinStartTime()
                                            + "-" + aggregator.getMaxEndTime()
                                            + ": "
                                            + aggregator.getSource().name()
                                            + ": ";
                                    switch (aggregator.getCategory()) {
                                    case READ:
                                        output += ((ReadDataOperationInfo.Aggregator) aggregator)
                                                .getData();
                                        break;
                                    case WRITE:
                                        output += ((ReadDataOperationInfo.Aggregator) aggregator)
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
