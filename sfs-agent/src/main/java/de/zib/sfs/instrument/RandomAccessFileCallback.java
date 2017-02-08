/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;

public class RandomAccessFileCallback {

    private final OperationStatisticsAggregator aggregator;

    public RandomAccessFileCallback() {
        // may be null during early phases of JVM initialization
        aggregator = OperationStatisticsAggregator.getInstance();
    }

    public void onOpenEnd(long startTime, long endTime) {
        if (aggregator != null) {
            aggregator.aggregate(new OperationStatistics(OperationSource.JVM,
                    OperationCategory.OTHER, startTime, endTime));
        }
    }

    public void onReadEnd(long startTime, long endTime, int readResult) {
        if (aggregator != null) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    endTime, readResult == -1 ? 0 : 1, null, false));
        }
    }

    public void onReadBytesEnd(long startTime, long endTime, int readBytesResult) {
        if (aggregator != null) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    endTime, readBytesResult == -1 ? 0 : readBytesResult, null,
                    false));
        }
    }

    public void onWriteEnd(long startTime, long endTime) {
        if (aggregator != null) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    endTime, 1));
        }
    }

    public void onWriteBytesEnd(long startTime, long endTime, int len) {
        if (aggregator != null) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    endTime, len));
        }
    }

}
