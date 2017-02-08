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

    public void onReadEnd(long startTime, long endTime, int readResult) {
        OperationStatisticsAggregator.instance
                .aggregate(new ReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : readResult, null, false));
    }

    public void onReadEnd(long startTime, long endTime, long readResult) {
        OperationStatisticsAggregator.instance
                .aggregate(new ReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : readResult, null, false));
    }

    public void onWriteEnd(long startTime, long endTime, int writeResult) {
        OperationStatisticsAggregator.instance
                .aggregate(new DataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime,
                        writeResult));
    }

    public void onWriteEnd(long startTime, long endTime, long writeResult) {
        OperationStatisticsAggregator.instance
                .aggregate(new DataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime,
                        writeResult));
    }

}
