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

    private final String fileUri;

    public WrappedFSDataInputStream(FSDataInputStream in, Logger logger,
            String fileUri) throws IOException {
        this.in = in;
        this.logger = logger;
        this.fileUri = fileUri;
    }

    @Override
    public int read() throws IOException {
        logger.info("readByte()@{}", fileUri);
        return in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        logger.info("readByteArray({},{},{})@{}", b.length, off, len, fileUri);
        return in.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        logger.info("readByteArray({})@{}", b.length, fileUri);
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
        logger.info("readByteArray({},{},{},{})@{}", position, buffer.length,
                offset, length, fileUri);
        return in.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        logger.info("readFullyByteArray({},{})@{}", position, buffer.length,
                fileUri);
        in.readFully(position, buffer);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        logger.info("readFullyByteArray({},{},{},{})@{}", position,
                buffer.length, offset, length, fileUri);
        in.readFully(position, buffer, offset, length);
    }

}
