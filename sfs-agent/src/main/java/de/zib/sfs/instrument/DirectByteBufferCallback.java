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

public class DirectByteBufferCallback {

    public void getCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 1, false);
    }

    public void getCallback(long startTime, long endTime, int length) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, length,
                        false);
    }

    public void getCharCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 2, false);
    }

    public void getDoubleCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 8, false);
    }

    public void getFloatCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 4, false);
    }

    public void getIntCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 4, false);
    }

    public void getLongCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 8, false);
    }

    public void getShortCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 2, false);
    }

    public void putCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 1);
    }

    public void putCallback(long startTime, long endTime, int length) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, length);
    }

    public void putCharCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 2);
    }

    public void putDoubleCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 8);
    }

    public void putFloatCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 4);
    }

    public void putIntCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 4);
    }

    public void putLongCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 8);
    }

    public void putShortCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 2);
    }

}
