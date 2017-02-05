/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.nio.ByteBuffer;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;
import sun.nio.ch.FileChannelImpl;

// FileChannelImpl is not supposed to be used
@SuppressWarnings("restriction")
public class FileChannelImplCallback {

    private final FileChannelImpl fci;
    private final Object parent;

    private final OperationStatisticsAggregator aggregator;

    public FileChannelImplCallback(FileChannelImpl fci, Object parent) {
        this.fci = fci;
        this.parent = parent;

        // may be null during early phases of JVM initialization
        aggregator = OperationStatisticsAggregator.getInstance();
    }

    public long onReadBegin(ByteBuffer dst) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onReadEnd(long startTime, int readResult, ByteBuffer dst) {
        if (startTime != -1L) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    System.currentTimeMillis(), readResult == -1 ? 0
                            : readResult, null, false));
        }
    }

    public long onReadBegin(ByteBuffer[] dsts, int offset, int length) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onReadEnd(long startTime, long readResult, ByteBuffer[] dsts,
            int offset, int length) {
        if (startTime != -1L) {
            aggregator.aggregate(new ReadDataOperationStatistics(
                    OperationSource.JVM, OperationCategory.READ, startTime,
                    System.currentTimeMillis(), readResult == -1 ? 0
                            : readResult, null, false));
        }
    }

    public long onWriteBegin(ByteBuffer src) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onWriteEnd(long startTime, int writeResult, ByteBuffer src) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    System.currentTimeMillis(), writeResult));
        }
    }

    public long onWriteBegin(ByteBuffer[] srcs, int offset, int length) {
        return aggregator != null ? System.currentTimeMillis() : -1L;
    }

    public void onWriteEnd(long startTime, long writeResult, ByteBuffer[] srcs,
            int offset, int length) {
        if (startTime != -1L) {
            aggregator.aggregate(new DataOperationStatistics(
                    OperationSource.JVM, OperationCategory.WRITE, startTime,
                    System.currentTimeMillis(), writeResult));
        }
    }

}
