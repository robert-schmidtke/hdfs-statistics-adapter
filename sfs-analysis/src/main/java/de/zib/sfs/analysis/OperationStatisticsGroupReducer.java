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

public class OperationStatisticsGroupReducer implements
        GroupReduceFunction<OperationStatistics, OperationStatistics> {

    private static final long serialVersionUID = -6279446327088687733L;

    private final long timeBinDuration;

    public OperationStatisticsGroupReducer(long timeBinDuration) {
        this.timeBinDuration = timeBinDuration;
    }

    @Override
    public void reduce(Iterable<OperationStatistics> values,
            final Collector<OperationStatistics> out) throws Exception {
        // some state to keep during iteration
        int pid = Integer.MIN_VALUE;
        long startTime = Long.MIN_VALUE;
        Map<Tuple2<OperationSource, OperationCategory>, OperationStatistics> aggregates = new HashMap<>();

        for (OperationStatistics value : values) {
            // get the current aggregate for this source and category
            Tuple2<OperationSource, OperationCategory> aggregateKey = Tuple2
                    .of(value.getSource(), value.getCategory());
            OperationStatistics aggregate = aggregates.get(aggregateKey);

            // The PIDs may change. This indicates a switch to a new file, which
            // begins with a new time.
            if (pid != value.getPid()) {
                pid = value.getPid();

                // if we have a current aggregate, emit it (will only be null on
                // the first iteration for each source/category and time bin)
                if (aggregate != null) {
                    out.collect(aggregate);
                }

                // start new aggregation for this source/category and time bin
                aggregate = value.clone();
                aggregates.put(aggregateKey, aggregate);

                startTime = value.getStartTime();
            } else {
                // same PID, but the time bin may be full
                if (value.getStartTime() - startTime >= timeBinDuration) {
                    // emit current aggregate and put the value in the next time
                    // bin
                    out.collect(aggregate);

                    aggregate = value.clone();
                    aggregates.put(aggregateKey, aggregate);

                    startTime = value.getStartTime();
                } else {
                    // just aggregate the current statistics
                    aggregate.add(value);
                }
            }
        }

        // collect remaining aggregates
        aggregates.forEach((key, v) -> out.collect(v));
    }

}
