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
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;

public class FileChannelImplCallback {

    private final OperationStatisticsAggregator aggregator;

    public FileChannelImplCallback() {
        // may be null during early phases of JVM initialization
        aggregator = OperationStatisticsAggregator.getInstance();
    }

    public void onReadEnd(long startTime, long endTime, int readResult) {
        if (startTime != -1L) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    endTime, readResult == -1 ? 0 : readResult, null, false));
        }
    }

    public void onReadEnd(long startTime, long endTime, long readResult) {
        if (startTime != -1L) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    endTime, readResult == -1 ? 0 : readResult, null, false));
        }
    }

    public void onWriteEnd(long startTime, long endTime, int writeResult) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    endTime, writeResult));
        }
    }

    public void onWriteEnd(long startTime, long endTime, long writeResult) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    endTime, writeResult));
        }
    }

}
