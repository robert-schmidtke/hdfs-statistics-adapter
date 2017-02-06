/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.RandomAccessFile;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;

public class RandomAccessFileCallback {

    private final RandomAccessFile raf;

    private final OperationStatisticsAggregator aggregator;

    public RandomAccessFileCallback(RandomAccessFile raf) {
        this.raf = raf;

        // may be null during early phases of JVM initialization
        aggregator = OperationStatisticsAggregator.getInstance();
    }

    public long onOpenBegin(String name, int mode) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onOpenEnd(long startTime, String name, int mode) {
        if (startTime != -1L) {
            aggregator.aggregate(new OperationStatistics(OperationSource.JVM,
                    OperationCategory.OTHER, startTime, System
                            .currentTimeMillis()));
        }
    }

    public long onReadBegin() {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onReadEnd(long startTime, int readResult) {
        if (startTime != -1L) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    System.currentTimeMillis(), readResult == -1 ? 0 : 1, null,
                    false));
        }
    }

    public long onReadBytesBegin(byte[] b, int off, int len) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onReadBytesEnd(long startTime, int readBytesResult, byte[] b,
            int off, int len) {
        if (startTime != -1L) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    System.currentTimeMillis(), readBytesResult == -1 ? 0
                            : readBytesResult, null, false));
        }
    }

    public long onWriteBegin(int b) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onWriteEnd(long startTime, int b) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    System.currentTimeMillis(), 1));
        }
    }

    public long onWriteBytesBegin(byte[] b, int off, int len) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onWriteBytesEnd(long startTime, byte[] b, int off, int len) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    System.currentTimeMillis(), len));
        }
    }

}
