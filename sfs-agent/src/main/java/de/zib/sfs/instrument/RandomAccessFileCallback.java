/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;

public class RandomAccessFileCallback {

    public void onOpenEnd(long startTime, long endTime) {
        OperationStatisticsAggregator.instance.aggregateOperationStatistics(
                OperationSource.JVM, OperationCategory.OTHER, startTime,
                endTime);
    }

    public void onReadEnd(long startTime, long endTime, int readResult) {
        OperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : 1, false);
    }

    public void onReadBytesEnd(long startTime, long endTime,
            int readBytesResult) {
        OperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readBytesResult == -1 ? 0 : readBytesResult, false);
    }

    public void onWriteEnd(long startTime, long endTime) {
        OperationStatisticsAggregator.instance.aggregateDataOperationStatistics(
                OperationSource.JVM, OperationCategory.WRITE, startTime,
                endTime, 1);
    }

    public void onWriteBytesEnd(long startTime, long endTime, int len) {
        OperationStatisticsAggregator.instance.aggregateDataOperationStatistics(
                OperationSource.JVM, OperationCategory.WRITE, startTime,
                endTime, len);
    }

}
