/*
 * Copyright (c) 2016 by Robert Schmidtke,
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
import sun.nio.ch.FileChannelImpl;

@SuppressWarnings("restriction")
public class FileChannelImplCallback extends AbstractSfsCallback {

    // set to true if write calls made to this callback should be discarded
    private boolean discard = false;

    /**
     * @param fci
     */
    public FileChannelImplCallback(FileChannelImpl fci) {
        // discard the file channel
    }

    public void openCallback(FileDescriptor fileDescriptor) {
        // see FileOutputStreamCallback
        this.discard = LiveOperationStatisticsAggregator.instance
                .isDiscardedFileDescriptor(fileDescriptor);
        if (!this.discard) {
            this.fd = LiveOperationStatisticsAggregator.instance
                    .getFileDescriptor(fileDescriptor);
        }
    }

    public void readCallback(long startTime, long endTime, int readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd,
                        readResult == -1 ? 0 : readResult, false);
    }

    public static void readCallback(FileDescriptor fileDescriptor,
            long startTime, long endTime, int readResult) {
        int fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(fileDescriptor);
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd,
                        readResult == -1 ? 0 : readResult, false);
    }

    public void readCallback(long startTime, long endTime, long readResult) {
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, this.fd,
                        readResult == -1 ? 0 : readResult, false);
    }

    public static void readCallback(FileDescriptor fileDescriptor,
            long startTime, long endTime, long readResult) {
        int fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(fileDescriptor);
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, fd,
                        readResult == -1 ? 0 : readResult, false);
    }

    public void writeCallback(long startTime, long endTime, int writeResult) {
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime,
                            this.fd, writeResult);
        }
    }

    public static void writeCallback(FileDescriptor fileDescriptor,
            long startTime, long endTime, int writeResult) {
        int fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(fileDescriptor);
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, fd,
                        writeResult);
    }

    public void writeCallback(long startTime, long endTime, long writeResult) {
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime,
                            this.fd, writeResult);
        }
    }

    public static void writeCallback(FileDescriptor fileDescriptor,
            long startTime, long endTime, long writeResult) {
        int fd = LiveOperationStatisticsAggregator.instance
                .getFileDescriptor(fileDescriptor);
        LiveOperationStatisticsAggregator.instance
                .aggregateDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.WRITE, startTime, endTime, fd,
                        writeResult);
    }

}
