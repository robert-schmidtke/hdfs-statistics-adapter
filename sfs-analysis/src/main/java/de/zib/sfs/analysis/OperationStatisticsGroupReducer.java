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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.statistics.OperationCategory;
import de.zib.sfs.analysis.statistics.OperationSource;
import de.zib.sfs.analysis.statistics.OperationStatistics;

public class OperationStatisticsGroupReducer
        implements
        GroupReduceFunction<OperationStatistics, OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -6279446327088687733L;

    private static final Logger LOG = LoggerFactory
            .getLogger(OperationStatisticsGroupReducer.class);

    private final long timeBinDuration;

    public OperationStatisticsGroupReducer(long timeBinDuration) {
        this.timeBinDuration = timeBinDuration;
    }

    @Override
    public void reduce(Iterable<OperationStatistics> values,
            final Collector<OperationStatistics.Aggregator> out)
            throws Exception {
        // some state to keep during iteration
        int pid = Integer.MIN_VALUE;
        long binStartTime = Long.MAX_VALUE;
        OperationStatistics lastValue = null;
        Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics.Aggregator> aggregators = new HashMap<>();

        for (OperationStatistics value : values) {
            // check if we already have an aggregator for this source and
            // category
            OperationStatistics.Aggregator currentAggregator = value
                    .getAggregator();
            Tuple2<OperationSource, OperationCategory> aggregatorKey = Tuple2
                    .of(currentAggregator.getSource(),
                            currentAggregator.getCategory());
            OperationStatistics.Aggregator aggregator = aggregators
                    .get(aggregatorKey);

            // The PIDs may change. This indicates a switch to a new file, which
            // begins with a new time.
            if (pid != value.getPid()) {
                pid = value.getPid();

                // if we have a current aggregate, emit it
                if (aggregator != null) {
                    out.collect(aggregator);
                }

                // start new aggregation for this source/category and time bin
                aggregators.put(aggregatorKey, currentAggregator);
                binStartTime = value.getStartTime();
                lastValue = value;
            } else {
                // some sanity checking on the non-decreasing property of time
                // on the input
                if (lastValue != null
                        && value.getStartTime() < lastValue.getStartTime()) {
                    throw new IllegalStateException(
                            "Current value cannot be earlier than the last value: "
                                    + value + ", " + lastValue);
                } else {
                    lastValue = value;
                }

                // start time is increasing within the file for the same PID
                binStartTime = Math.min(binStartTime, value.getStartTime());

                // same PID, but the time bin may be full
                if (value.getStartTime() - binStartTime >= timeBinDuration) {
                    // emit current aggregate and put the value in the next time
                    // bin
                    if (aggregator != null) {
                        out.collect(aggregator);
                    }
                    aggregators.put(aggregatorKey, currentAggregator);
                    binStartTime = value.getStartTime();
                } else {
                    // just aggregate the current statistics
                    if (aggregator != null) {
                        try {
                            aggregator.aggregate(currentAggregator);
                        } catch (OperationStatistics.Aggregator.NotAggregatableException e) {
                            LOG.warn("Could not aggregate statistics: {}",
                                    e.getMessage());
                        }
                    } else {
                        aggregators.put(aggregatorKey, currentAggregator);
                    }
                }
            }
        }

        // collect remaining aggregates
        aggregators.forEach((k, v) -> out.collect(v));
    }

}
