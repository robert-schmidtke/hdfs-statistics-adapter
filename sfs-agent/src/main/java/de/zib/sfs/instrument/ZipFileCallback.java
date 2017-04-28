/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class ZipFileCallback {

    private static final Map<Long, Long> ZIP_CACHE = new ConcurrentHashMap<>();

    public static void constructorCallback(long startTime, long endTime,
            long jzfile, long length) {
        // ZipFile caches ZIP files, so we need to count them only once as well
        if (ZIP_CACHE.merge(jzfile, 1L, (v1, v2) -> v1 + v2) == 1L) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateReadDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.ZIP, startTime, endTime,
                            length, false);

            // For testing purposes, keep track of how much data was read using
            // ZipFiles. Automatically disabled in non-assertion-enabled
            // environments.
            assert (incrementTotalData(length));
        }
    }

    public static void closeCallback(long jzfile) {
        // remove ZIP file from cache if its reference count reaches 0
        ZIP_CACHE.merge(jzfile, -1L, (v1, v2) -> v1 + v2 == 0 ? null : v1 + v2);
    }

    private static long totalData = 0;

    private static boolean incrementTotalData(long data) {
        totalData += data;
        return true;
    }

    public static long getTotalData() {
        return totalData;
    }

}
