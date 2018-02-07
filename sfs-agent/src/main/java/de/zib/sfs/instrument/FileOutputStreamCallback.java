/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class FileOutputStreamCallback extends AbstractSfsCallback {

    // the LiveOperationStatisticsAggregator uses FileOutputStream or
    // FileChannels obtained from FileOutputStreams to write its logs
    private static String LOG_FILE_PREFIX = null;

    private FileOutputStream fos;

    public FileOutputStreamCallback(FileOutputStream fos) {
        // fos.getFD() still throws at this point
        this.fos = fos;
    }

    public void openCallback(long startTime, long endTime, String filename) {
        if (this.discard) {
            return;
        }

        try {
            this.fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(filename, this.fos.getFD());
            this.fos = null;
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

    public void writeCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime,
                            this.fd, 1);
        }
    }

    public void writeBytesCallback(long startTime, long endTime, int len) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime,
                            this.fd, len);
        }
    }

    private void getFileDescriptor() {
        if (this.fd != -1) {
            return;
        }

        try {
            if (this.fos == null) {
                return;
            }
            synchronized (this) {
                if (this.fos == null) {
                    return;
                }

                FileDescriptor fileDescriptor = this.fos.getFD();
                this.discard |= FileDescriptor.out.equals(fileDescriptor)
                        || FileDescriptor.err.equals(fileDescriptor);
                this.fd = LiveOperationStatisticsAggregator.instance
                        .getFileDescriptor(fileDescriptor);
                this.fos = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
