/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileInputStream;
import java.io.IOException;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class FileInputStreamCallback extends AbstractSfsCallback {

    private final FileInputStream fis;

    public FileInputStreamCallback(FileInputStream fis) {
        this.fis = fis;
    }

    public void openCallback(long startTime, long endTime, String filename) {
        fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(filename);
        putFileDescriptor();
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.JVM,
                            OperationCategory.OTHER, startTime, endTime, fd);
        }
    }

    public void readCallback(long startTime, long endTime, int readResult) {
        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd,
                        readResult == -1 ? 0 : 1, false);
    }

    public void readBytesCallback(long startTime, long endTime,
            int readResult) {
        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd,
                        readResult == -1 ? 0 : readResult, false);
    }

    private void putFileDescriptor() {
        try {
            putFileDescriptor(fis.getFD(), fd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void getFileDescriptor() {
        if (fd != -1) {
            return;
        }

        try {
            fd = getFileDescriptor(fis.getFD());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
