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

    public void onGetEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 1, false);
    }

    public void onGetEnd(long startTime, long endTime, int length) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, length,
                        false);
    }

    public void onGetCharEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 2, false);
    }

    public void onGetDoubleEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 8, false);
    }

    public void onGetFloatEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 4, false);
    }

    public void onGetIntEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 4, false);
    }

    public void onGetLongEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 8, false);
    }

    public void onGetShortEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, 2, false);
    }

    public void onPutEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 1);
    }

    public void onPutEnd(long startTime, long endTime, int length) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, length);
    }

    public void onPutCharEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 2);
    }

    public void onPutDoubleEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 8);
    }

    public void onPutFloatEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 4);
    }

    public void onPutIntEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 4);
    }

    public void onPutLongEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 8);
    }

    public void onPutShortEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 2);
    }

}
