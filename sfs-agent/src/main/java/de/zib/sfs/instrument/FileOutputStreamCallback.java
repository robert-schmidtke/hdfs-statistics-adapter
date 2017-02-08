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

public class FileOutputStreamCallback {

    private final OperationStatisticsAggregator aggregator;

    public FileOutputStreamCallback() {
        // may be null during early phases of JVM initialization
        aggregator = OperationStatisticsAggregator.getInstance();
    }

    public void onOpenEnd(long startTime, long endTime) {
        if (aggregator != null) {
            aggregator.aggregate(new OperationStatistics(OperationSource.JVM,
                    OperationCategory.OTHER, startTime, endTime));
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
