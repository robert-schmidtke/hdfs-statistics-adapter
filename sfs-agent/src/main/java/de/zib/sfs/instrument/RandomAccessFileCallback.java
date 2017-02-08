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

    public void onOpenEnd(long startTime, long endTime) {
        OperationStatisticsAggregator.instance
                .aggregate(new OperationStatistics(OperationSource.JVM,
                        OperationCategory.OTHER, startTime, endTime));
    }

    public void onReadEnd(long startTime, long endTime, int readResult) {
        OperationStatisticsAggregator.instance
                .aggregate(new ReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : 1, null, false));
    }

    public void onReadBytesEnd(long startTime, long endTime, int readBytesResult) {
        OperationStatisticsAggregator.instance
                .aggregate(new ReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readBytesResult == -1 ? 0 : readBytesResult, null,
                        false));
    }

    public void onWriteEnd(long startTime, long endTime) {
        OperationStatisticsAggregator.instance
                .aggregate(new DataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, 1));
    }

    public void onWriteBytesEnd(long startTime, long endTime, int len) {
        OperationStatisticsAggregator.instance
                .aggregate(new DataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, len));
    }

}
