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
import org.apache.logging.log4j.Logger;

public class WrappedFSDataOutputStream extends FSDataOutputStream {

    private final Logger logger;

    public WrappedFSDataOutputStream(OutputStream out, Logger logger)
            throws IOException {
        this(out, null, 0, logger);
    }

    public WrappedFSDataOutputStream(OutputStream out, Statistics stats,
            long startPosition, Logger logger) throws IOException {
        super(out, stats, startPosition);
        this.logger = logger;
    }

    @Override
    public synchronized void write(int b) throws IOException {
        long startTime = System.currentTimeMillis();
        super.write(b);
        long duration = System.currentTimeMillis() - startTime;
        //logger.info("{}-{}:{}.write({}):void", startTime, duration, this, b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        long startTime = System.currentTimeMillis();
        super.write(b);
        long duration = System.currentTimeMillis() - startTime;
        //logger.info("{}-{}:{}.write([{}]):void", startTime, duration, this,
        //        b.length);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len)
            throws IOException {
        long startTime = System.currentTimeMillis();
        super.write(b, off, len);
        long duration = System.currentTimeMillis() - startTime;
        //logger.info("{}-{}:{}.write([{}],{},{}):void", startTime, duration,
        //        this, b.length, off, len);
    }

}
