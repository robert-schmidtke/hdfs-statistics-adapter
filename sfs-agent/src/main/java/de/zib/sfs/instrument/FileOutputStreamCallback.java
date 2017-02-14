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
import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;

public class FileOutputStreamCallback {

    public void onOpenEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance.aggregateOperationStatistics(
                OperationSource.JVM, OperationCategory.OTHER, startTime,
                endTime);
    }

    public void onWriteEnd(long startTime, long endTime) {
        LiveOperationStatisticsAggregator.instance.aggregateDataOperationStatistics(
                OperationSource.JVM, OperationCategory.WRITE, startTime,
                endTime, 1);
    }

    public void onWriteBytesEnd(long startTime, long endTime, int len) {
        LiveOperationStatisticsAggregator.instance.aggregateDataOperationStatistics(
                OperationSource.JVM, OperationCategory.WRITE, startTime,
                endTime, len);
    }

}
