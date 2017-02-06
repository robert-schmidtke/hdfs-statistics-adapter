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

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;

public class WrappedFSDataOutputStream extends FSDataOutputStream {

    private final OperationStatisticsAggregator aggregator;

    public WrappedFSDataOutputStream(OutputStream out,
            OperationStatisticsAggregator aggregator) throws IOException {
        this(out, null, 0, aggregator);
    }

    public WrappedFSDataOutputStream(OutputStream out, Statistics stats,
            long startPosition, OperationStatisticsAggregator aggregator)
            throws IOException {
        super(out, stats, startPosition);
        this.aggregator = aggregator;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        long startTime = System.currentTimeMillis();
        super.write(b);
        aggregator.aggregate(new DataOperationStatistics(OperationSource.SFS,
                OperationCategory.WRITE, startTime, System.currentTimeMillis(),
                1));
    }

    @Override
    public void write(byte[] b) throws IOException {
        long startTime = System.currentTimeMillis();
        super.write(b);
        aggregator.aggregate(new DataOperationStatistics(OperationSource.SFS,
                OperationCategory.WRITE, startTime, System.currentTimeMillis(),
                b.length));
    }

    @Override
    public synchronized void write(byte[] b, int off, int len)
            throws IOException {
        long startTime = System.currentTimeMillis();
        super.write(b, off, len);
        aggregator.aggregate(new DataOperationStatistics(OperationSource.SFS,
                OperationCategory.WRITE, startTime, System.currentTimeMillis(),
                len));
    }

}
