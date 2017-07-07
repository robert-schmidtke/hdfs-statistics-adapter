/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class RandomAccessFileCallback {

    private int fd = -1;

    public void openCallback(long startTime, long endTime, String filename) {
        fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(filename);
        LiveOperationStatisticsAggregator.instance.aggregateOperationStatistics(
                OperationSource.JVM, OperationCategory.OTHER, startTime,
                endTime);
    }

    public void readCallback(long startTime, long endTime, int readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : 1, false);
    }

    public void readBytesCallback(long startTime, long endTime,
            int readBytesResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readBytesResult == -1 ? 0 : readBytesResult, false);
    }

    public void writeCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 1);
    }

    public void writeBytesCallback(long startTime, long endTime, int len) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, len);
    }

}
