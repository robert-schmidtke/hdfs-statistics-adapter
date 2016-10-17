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
import org.apache.logging.log4j.Logger;

public class WrappedFSDataInputStream extends InputStream implements
        PositionedReadable, Seekable {

    private final FSDataInputStream in;

    private final Logger logger;

    public WrappedFSDataInputStream(FSDataInputStream in, Logger logger)
            throws IOException {
        this.in = in;
        this.logger = logger;
    }

    @Override
    public int read() throws IOException {
        logger.info("read()");
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        logger.info("read({},{},{})", b.length, off, len);
        return in.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        logger.info("read({})", b.length);
        return in.read(b);
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long desired) throws IOException {
        in.seek(desired);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return in.seekToNewSource(targetPos);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        logger.info("read({},{},{},{})", position, buffer.length, offset,
                length);
        return in.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        logger.info("readFully({},{})", position, buffer.length);
        in.readFully(position, buffer);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        logger.info("readFully({},{},{},{})", position, buffer.length, offset,
                length);
        in.readFully(position, buffer, offset, length);
    }

}
