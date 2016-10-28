/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.logging.log4j.Logger;

public class WrappedFSDataInputStream extends InputStream implements
        PositionedReadable, Seekable {

    private final FSDataInputStream in;

    private final HdfsDataInputStream hdfsIn;

    private final Logger logger;

    public WrappedFSDataInputStream(FSDataInputStream in, Logger logger)
            throws IOException {
        this.in = in;
        this.logger = logger;

        if (in instanceof HdfsDataInputStream) {
            hdfsIn = (HdfsDataInputStream) in;
        } else {
            hdfsIn = null;
        }
    }

    @Override
    public int read() throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read();
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read():{}", duration, this,
                getCurrentDataNodeString(), result);
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b, off, len);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read([{}],{},{}):{}", duration, this,
                getCurrentDataNodeString(), b.length, off, len, result);
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read([{}]):{}", duration, this,
                getCurrentDataNodeString(), b.length, result);
        return result;
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long desired) throws IOException {
        long startTime = System.currentTimeMillis();
        in.seek(desired);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.seek({}):void", duration, this,
                getCurrentDataNodeString(), desired);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        long startTime = System.currentTimeMillis();
        boolean result = in.seekToNewSource(targetPos);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.seekToNewSource({}):{}", duration, this,
                getCurrentDataNodeString(), targetPos, result);
        return result;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(position, buffer, offset, length);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read({},[{}],{},{}):{}", duration, this,
                getCurrentDataNodeString(), position, buffer.length, offset,
                length, result);
        return result;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.readFully({},[{}]):void", duration, this,
                getCurrentDataNodeString(), position, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer, offset, length);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.readFully({},[{}],{},{}):void", duration, this,
                getCurrentDataNodeString(), position, buffer.length, offset,
                length);
    }

    // Helper methods
    private String getCurrentDataNodeString() {
        if (hdfsIn == null) {
            return "";
        } else {
            return "->" + hdfsIn.getCurrentDatanode().getHostName();
        }
    }

}
