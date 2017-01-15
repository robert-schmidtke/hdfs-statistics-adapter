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

public class AggregatedOperationStatisticsGroupReducer
        implements
        GroupReduceFunction<OperationStatistics.Aggregator, OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -6279446327088687733L;

    private static final Logger LOG = LoggerFactory
            .getLogger(AggregatedOperationStatisticsGroupReducer.class);

    private final long timeBinDuration;

    public AggregatedOperationStatisticsGroupReducer(long timeBinDuration) {
        this.timeBinDuration = timeBinDuration;
    }

    @Override
    public void reduce(Iterable<OperationStatistics.Aggregator> values,
            final Collector<OperationStatistics.Aggregator> out)
            throws Exception {
        // some state to keep during iteration
        long binStartTime = Long.MAX_VALUE;
        OperationStatistics.Aggregator lastValue = null;
        Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics.Aggregator> aggregators = new HashMap<>();

        for (OperationStatistics.Aggregator value : values) {
            // some sanity checking on the non-decreasing property of time on
            // the input
            if (lastValue != null
                    && value.getStartTime() < lastValue.getStartTime()) {
                throw new IllegalStateException(
                        "Current start time cannot be smaller than the last start time: "
                                + value + ", " + lastValue);
            } else {
                lastValue = value;
            }

            Tuple2<OperationSource, OperationCategory> aggregatorKey = Tuple2
                    .of(value.getSource(), value.getCategory());
            OperationStatistics.Aggregator aggregator = aggregators
                    .get(aggregatorKey);

            binStartTime = Math.min(binStartTime, value.getStartTime());
            if (value.getStartTime() - binStartTime >= timeBinDuration) {
                // emit current aggregate and put the value in the next time
                // bin
                if (aggregator != null) {
                    out.collect(aggregator);
                }
                aggregators.put(aggregatorKey, value);
                binStartTime = value.getStartTime();
                lastValue = value;
            } else {
                // just aggregate the current statistics
                if (aggregator != null) {
                    try {
                        aggregator.aggregate(value);
                    } catch (OperationStatistics.Aggregator.NotAggregatableException e) {
                        LOG.warn("Could not aggregate statistics: {}",
                                e.getMessage());
                    }
                } else {
                    aggregators.put(aggregatorKey, value);
                }
            }
        }

        // collect remaining aggregates
        aggregators.forEach((k, v) -> out.collect(v));
    }

}
