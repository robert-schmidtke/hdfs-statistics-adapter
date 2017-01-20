/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

import java.util.TreeMap;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.statistics.OperationStatistics;

public class AggregatedOperationStatisticsAggregator
        implements
        GroupReduceFunction<OperationStatistics.Aggregator, OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -6279446327088687733L;

    private static final Logger LOG = LoggerFactory
            .getLogger(AggregatedOperationStatisticsAggregator.class);

    private final int timeBinCacheSize;

    public AggregatedOperationStatisticsAggregator(int timeBinCacheSize) {
        this.timeBinCacheSize = timeBinCacheSize;
    }

    @Override
    public void reduce(Iterable<OperationStatistics.Aggregator> values,
            final Collector<OperationStatistics.Aggregator> out)
            throws Exception {
        // map of time bins to use as cache before emitting to allow for late
        // arrivals of OperationStatistics
        TreeMap<Long, OperationStatistics.Aggregator> aggregators = new TreeMap<>();

        for (OperationStatistics.Aggregator value : values) {
            // get the time bin applicable for this operation
            OperationStatistics.Aggregator aggregator = aggregators.get(value
                    .getTimeBin());
            if (aggregator == null) {
                // add new bin if we have the space
                if (aggregators.size() < timeBinCacheSize) {
                    aggregators.put(value.getTimeBin(), value);
                } else {
                    LOG.warn(
                            "Dropping record: {} because it arrived too late, current time span is: {} - {}",
                            value, aggregators.firstKey(),
                            aggregators.lastKey());
                    continue;
                }
            } else {
                // aggregate the statistics
                aggregator.aggregate(value);
            }

            // make sure to emit aggregates when the cache is full
            while (aggregators.size() >= timeBinCacheSize) {
                out.collect(aggregators.remove(aggregators.firstKey()));
            }
        }

        // collect remaining aggregates
        aggregators.forEach((k, v) -> out.collect(v));
    }

}
