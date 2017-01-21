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
import org.apache.flink.api.java.tuple.Tuple4;
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
        TreeMap<Long, Map<Tuple4<Integer, String, OperationSource, OperationCategory>, OperationStatistics.Aggregator>> aggregators = new TreeMap<>();

        String hostname = null;
        for (OperationStatistics value : values) {
            // check that we process one host only
            if (hostname == null) {
                hostname = value.getHostname();
            } else if (!hostname.equals(value.getHostname())) {
                throw new IllegalStateException("Unexpected hostname "
                        + value.getHostname() + ", expected " + hostname);
            }

            // get the time bin applicable for this operation
            OperationStatistics.Aggregator aggregator = value
                    .getAggregator(timeBinDuration);
            Map<Tuple4<Integer, String, OperationSource, OperationCategory>, OperationStatistics.Aggregator> aggregatorsPerTimeBin = aggregators
                    .get(aggregator.getTimeBin());
            if (aggregatorsPerTimeBin == null) {
                // add new bin if we have the space
                if (aggregators.size() < timeBinCacheSize) {
                    aggregatorsPerTimeBin = new HashMap<>();
                    aggregators.put(aggregator.getTimeBin(),
                            aggregatorsPerTimeBin);
                } else {
                    LOG.warn(
                            "Dropping record: {} because it arrived too late, current time span is: {} - {}",
                            value, aggregators.firstKey(),
                            aggregators.lastKey());
                    continue;
                }
            }

            // aggregate the statistics for the time bin, pid, key, source and
            // category
            aggregatorsPerTimeBin
                    .merge(Tuple4.of(value.getPid(), value.getKey(),
                            value.getSource(), value.getCategory()),
                            aggregator,
                            (v1, v2) -> {
                                try {
                                    return v1.aggregate(v2);
                                } catch (OperationStatistics.Aggregator.NotAggregatableException e) {
                                    throw new RuntimeException(e);
                                }
                            });

            // make sure to emit aggregates when the cache is full
            while (aggregators.size() >= timeBinCacheSize) {
                aggregators.remove(aggregators.firstKey()).forEach(
                        (k, v) -> out.collect(v));
            }
        }

        // collect remaining aggregates
        aggregators.forEach((k, v) -> v.forEach((k1, v1) -> out.collect(v1)));
    }
}
