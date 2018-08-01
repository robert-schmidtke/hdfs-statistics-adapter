/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.Path;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class WrappedFSDataOutputStream extends FSDataOutputStream {

    private final int fd;

    private final LiveOperationStatisticsAggregator aggregator;

    public WrappedFSDataOutputStream(OutputStream out, Path f,
            LiveOperationStatisticsAggregator aggregator) throws IOException {
        this(out, f, null, 0, aggregator);
    }

    public WrappedFSDataOutputStream(OutputStream out, Path f, Statistics stats,
            long startPosition, LiveOperationStatisticsAggregator aggregator)
            throws IOException {
        super(out, stats, startPosition);
        this.aggregator = aggregator;
        this.fd = this.aggregator.registerFileDescriptor(f.toString());
    }

    @Override
    public synchronized void write(int b) throws IOException {
        long startTime = System.nanoTime();
        super.write(b);
        this.aggregator.aggregateDataOperationStatistics(OperationSource.SFS,
                OperationCategory.WRITE, startTime, System.nanoTime(), this.fd,
                1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        long startTime = System.nanoTime();
        super.write(b);
        this.aggregator.aggregateDataOperationStatistics(OperationSource.SFS,
                OperationCategory.WRITE, startTime, System.nanoTime(), this.fd,
                b.length);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len)
            throws IOException {
        long startTime = System.nanoTime();
        super.write(b, off, len);
        this.aggregator.aggregateDataOperationStatistics(OperationSource.SFS,
                OperationCategory.WRITE, startTime, System.nanoTime(), this.fd,
                len);
    }

}
