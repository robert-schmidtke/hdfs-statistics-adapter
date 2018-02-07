/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class FileInputStreamCallback extends AbstractSfsCallback {

    private FileInputStream fis;

    public FileInputStreamCallback(FileInputStream fis) {
        this.fis = fis;
    }

    public void openCallback(long startTime, long endTime, String filename) {
        if (this.discard) {
            return;
        }

        try {
            this.fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(filename, this.fis.getFD());
            this.fis = null;
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
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateReadDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.READ, startTime, endTime, this.fd,
                            readResult == -1 ? 0 : 1, false);
        }
    }

    public void readBytesCallback(long startTime, long endTime,
            int readResult) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateReadDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.READ, startTime, endTime, this.fd,
                            readResult == -1 ? 0 : readResult, false);
        }
    }

    private void getFileDescriptor() {
        if (this.fd != -1) {
            return;
        }

        try {
            if (this.fis == null) {
                return;
            }
            synchronized (this) {
                if (this.fis == null) {
                    return;
                }

                FileDescriptor fileDescriptor = this.fis.getFD();
                this.discard = FileDescriptor.in.equals(fileDescriptor);
                this.fd = LiveOperationStatisticsAggregator.instance
                        .getFileDescriptor(fileDescriptor);
                this.fis = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
