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

public class FileChannelImplCallback {

    public void onReadEnd(long startTime, long endTime, int readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : readResult, false);
    }

    public void onReadEnd(long startTime, long endTime, long readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        readResult == -1 ? 0 : readResult, false);
    }

    public void onWriteEnd(long startTime, long endTime, int writeResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime,
                        writeResult);
    }

    public void onWriteEnd(long startTime, long endTime, long writeResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime,
                        writeResult);
    }

    public void onTransferToEnd(long startTime, long endTime,
            long transferResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime,
                        transferResult, false);
    }

    public void onTransferFromEnd(long startTime, long endTime,
            long transferResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime,
                        transferResult);
    }

}
