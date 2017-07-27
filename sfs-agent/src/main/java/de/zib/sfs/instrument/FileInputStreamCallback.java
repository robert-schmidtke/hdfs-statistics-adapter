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

public class FileInputStreamCallback {

    public void openCallback(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance.aggregateOperationStatistics(
                OperationSource.JVM, OperationCategory.OTHER, startTime,
                endTime, fd);
    }

    public void readCallback(long startTime, long endTime, int readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd,
                        readResult == -1 ? 0 : 1, false);
    }

    public void readBytesCallback(long startTime, long endTime,
            int readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd,
                        readResult == -1 ? 0 : readResult, false);
    }

}
