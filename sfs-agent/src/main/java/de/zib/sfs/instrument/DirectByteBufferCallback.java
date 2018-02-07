/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class DirectByteBufferCallback extends AbstractSfsCallback {

    private FileDescriptor fileDescriptor;

    public DirectByteBufferCallback() {
        // only no-arg constructor allowed
    }

    public void openCallback(FileDescriptor fileDescriptor) {
        this.fileDescriptor = fileDescriptor;
    }

    public void getCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 1,
                        false);
    }

    public void getCallback(long startTime, long endTime, int length) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd,
                        length, false);
    }

    // get callback to act on behalf of another file descriptor
    // convenient in DirectByteBuffer
    public static void getCallback(FileDescriptor fileDescriptor,
            long startTime, long endTime, int length) {
        int fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(fileDescriptor);
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd, length,
                        false);
    }

    public void getCharCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 2,
                        false);
    }

    public void getDoubleCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 8,
                        false);
    }

    public void getFloatCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 4,
                        false);
    }

    public void getIntCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 4,
                        false);
    }

    public void getLongCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 8,
                        false);
    }

    public void getShortCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd, 2,
                        false);
    }

    public void putCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        1);
    }

    public void putCallback(long startTime, long endTime, int length) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        length);
    }

    public void putCharCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        2);
    }

    public void putDoubleCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        8);
    }

    public void putFloatCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        4);
    }

    public void putIntCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        4);
    }

    public void putLongCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        8);
    }

    public void putShortCallback(long startTime, long endTime) {
        if (this.discard) {
            return;
        }

        getFileDescriptor();
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, this.fd,
                        2);
    }

    private void getFileDescriptor() {
        if (this.fd != -1) {
            return;
        }

        synchronized (this) {
            if (this.fd != -1) {
                return;
            }
            this.fd = this.fileDescriptor == null ? 0
                    : LiveOperationStatisticsAggregator.instance
                            .getFileDescriptor(this.fileDescriptor);
        }
        this.fileDescriptor = null;
    }

}
