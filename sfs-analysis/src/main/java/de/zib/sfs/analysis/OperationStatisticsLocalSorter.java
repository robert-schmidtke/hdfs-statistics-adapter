/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

import java.util.TreeMap;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;

import de.zib.sfs.analysis.statistics.OperationStatistics;

/**
 * Keeps a configurable number of {@link OperationStatistics} in a locally
 * time-sorted cache and emits them in correct order once the cache is full.
 * 
 * @author robert
 *
 */
public class OperationStatisticsLocalSorter implements
        MapPartitionFunction<OperationStatistics, OperationStatistics> {

    private static final long serialVersionUID = -1555432151716733225L;

    private final int localCacheSize;

    public OperationStatisticsLocalSorter(int localCacheSize) {
        this.localCacheSize = localCacheSize;
    }

    @Override
    public void mapPartition(Iterable<OperationStatistics> values,
            Collector<OperationStatistics> out) throws Exception {
        // some state to keep during iterations
        int pid = Integer.MIN_VALUE;
        TreeMap<Long, OperationStatistics> operationStatistics = new TreeMap<>();

        for (OperationStatistics value : values) {
            if (pid != value.getPid()) {
                // a change in PIDs means a new file and a new start time, so
                // emit everything that we have so far
                pid = value.getPid();
                operationStatistics.values().forEach(v -> out.collect(v));
                operationStatistics.clear();
            } else {
                // insert the current value into the right place
                operationStatistics.put(value.getStartTime(), value);

                // if the cache is full, emit elements until it is of proper
                // size again
                while (operationStatistics.size() > localCacheSize) {
                    out.collect(operationStatistics.remove(operationStatistics
                            .firstKey()));
                }
            }
        }

        // emit the remaining operation statistics
        operationStatistics.values().forEach(v -> out.collect(v));
    }
}
