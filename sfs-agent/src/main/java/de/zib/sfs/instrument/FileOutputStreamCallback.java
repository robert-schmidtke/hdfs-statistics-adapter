/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileOutputStream;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;

public class FileOutputStreamCallback {

    private final FileOutputStream fos;

    private final OperationStatisticsAggregator aggregator;

    public FileOutputStreamCallback(FileOutputStream fos) {
        this.fos = fos;

        // may be null during early phases of JVM initialization
        aggregator = OperationStatisticsAggregator.getInstance();
    }

    public long onOpenBegin(String name, boolean append) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onOpenEnd(long startTime, String name, boolean append) {
        if (startTime != -1L) {
            aggregator.aggregate(new OperationStatistics(OperationSource.JVM,
                    OperationCategory.OTHER, startTime, System
                            .currentTimeMillis()));
        }
    }

    public long onWriteBegin(int b, boolean append) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onWriteEnd(long startTime, int b, boolean append) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    System.currentTimeMillis(), 1));
        }
    }

    public long onWriteBytesBegin(byte[] b, int off, int len, boolean append) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onWriteBytesEnd(long startTime, byte[] b, int off, int len,
            boolean append) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    System.currentTimeMillis(), len));
        }
    }

}
