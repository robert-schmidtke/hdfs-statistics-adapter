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

    private final String fileUri;

    public WrappedFSDataOutputStream(OutputStream out, Logger logger,
            String fileUri) throws IOException {
        this(out, null, 0, logger, fileUri);
    }

    public WrappedFSDataOutputStream(OutputStream out, Statistics stats,
            long startPosition, Logger logger, String fileUri)
            throws IOException {
        super(out, stats, startPosition);
        this.logger = logger;
        this.fileUri = fileUri;
    }

    @Override
    public void write(byte[] b) throws IOException {
        logger.info("writeByteArray({})@{}", b.length, fileUri);
        super.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len)
            throws IOException {
        logger.info("writeByteArray({},{},{})@{}", b.length, off, len, fileUri);
        super.write(b, off, len);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        logger.info("writeByte({})@{}", b, fileUri);
        super.write(b);
    }

}
