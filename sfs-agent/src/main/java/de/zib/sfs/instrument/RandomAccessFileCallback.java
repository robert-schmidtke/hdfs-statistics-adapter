/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.IOException;
import java.io.RandomAccessFile;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class RandomAccessFileCallback extends AbstractSfsCallback {

    private RandomAccessFile raf;

    public RandomAccessFileCallback(RandomAccessFile raf) {
        this.raf = raf;
    }

    public void openCallback(long startTime, long endTime, String filename) {
        try {
            this.fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(filename, this.raf.getFD());
            this.raf = null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!this.skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.JVM,
                            OperationCategory.OTHER, startTime, endTime,
                            this.fd);
        }
    }

    public void readCallback(long startTime, long endTime, int readResult) {
        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd,
                        readResult == -1 ? 0 : 1, false);
    }

    public void readBytesCallback(long startTime, long endTime,
            int readBytesResult) {
        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd,
                        readBytesResult == -1 ? 0 : readBytesResult, false);
    }

    public void writeCallback(long startTime, long endTime) {
        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        1);
    }

    public void writeBytesCallback(long startTime, long endTime, int len) {
        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        len);
    }

    private void getFileDescriptor() {
        if (this.fd != -1) {
            return;
        }

        try {
            synchronized (this) {
                if (this.raf == null) {
                    return;
                }

                this.fd = LiveOperationStatisticsAggregator.instance
                        .getFileDescriptor(this.raf.getFD());
                this.raf = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
