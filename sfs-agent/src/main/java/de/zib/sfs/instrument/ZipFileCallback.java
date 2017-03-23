/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class ZipFileCallback {

    public static void constructorCallback(long startTime, long endTime,
            long data) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, data,
                        false);

        // For testing purposes, keep track of how much data was read using
        // ZipFiles. Automatically disabled in non-assertion-enabled
        // environments.
        assert (incrementTotalData(data));
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
