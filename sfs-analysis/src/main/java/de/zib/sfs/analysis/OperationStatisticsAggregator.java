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
import java.util.TreeMap;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.statistics.OperationCategory;
import de.zib.sfs.analysis.statistics.OperationSource;
import de.zib.sfs.analysis.statistics.OperationStatistics;

public class OperationStatisticsAggregator
        implements
        GroupReduceFunction<OperationStatistics, OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -6279446327088687733L;

    private static final Logger LOG = LoggerFactory
            .getLogger(OperationStatisticsAggregator.class);

    private final long timeBinDuration;

    private final int timeBinCacheSize;

    public OperationStatisticsAggregator(long timeBinDuration,
            int timeBinCacheSize) {
        this.timeBinDuration = timeBinDuration;
        this.timeBinCacheSize = timeBinCacheSize;
    }

    @Override
    public void reduce(Iterable<OperationStatistics> values,
            final Collector<OperationStatistics.Aggregator> out)
            throws Exception {
        // map of time bins to use as cache before emitting to allow for late
        // arrivals of OperationStatistics
        TreeMap<Long, Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics.Aggregator>> timeBinnedAggregators = new TreeMap<>();

        for (OperationStatistics value : values) {
            // get the time bin applicable for this operation
            long timeBin = value.getStartTime() - value.getStartTime()
                    % timeBinDuration;
            Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics.Aggregator> sourceCategorizedAggregators = timeBinnedAggregators
                    .get(timeBin);
            if (sourceCategorizedAggregators == null) {
                // add new bin if we have the space
                if (timeBinnedAggregators.size() < timeBinCacheSize) {
                    Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics.Aggregator> sourceCategorizedAggregator = new HashMap<>();
                    sourceCategorizedAggregator.put(
                            Tuple2.of(value.getSource(), value.getCategory()),
                            value.getAggregator());
                    timeBinnedAggregators.put(timeBin,
                            sourceCategorizedAggregator);
                } else {
                    LOG.warn(
                            "Dropping record: {} because it arrived too late, minimum current time is: {}",
                            value, timeBinnedAggregators.firstKey());
                }
            } else {
                // just aggregate the statistics
                Tuple2<OperationSource, OperationCategory> key = Tuple2.of(
                        value.getSource(), value.getCategory());
                OperationStatistics.Aggregator sourceCategorizedAggregator = sourceCategorizedAggregators
                        .get(key);
                if (sourceCategorizedAggregator == null) {
                    sourceCategorizedAggregator = value.getAggregator();
                    sourceCategorizedAggregators.put(key,
                            sourceCategorizedAggregator);
                } else {
                    sourceCategorizedAggregator
                            .aggregate(value.getAggregator());
                }
            }

            // make sure to emit aggregates when the cache is full
            while (timeBinnedAggregators.size() >= timeBinCacheSize) {
                timeBinnedAggregators.remove(timeBinnedAggregators.firstKey())
                        .forEach((k, v) -> out.collect(v));
            }
        }

        // collect remaining aggregates
        timeBinnedAggregators.forEach((k, v) -> v.forEach((k1, v1) -> out
                .collect(v1)));
    }
}
