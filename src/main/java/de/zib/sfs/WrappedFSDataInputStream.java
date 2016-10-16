/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.EnumSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.logging.log4j.Logger;

public class WrappedFSDataInputStream extends FSDataInputStream {

    private final Logger logger;

    public WrappedFSDataInputStream(InputStream in, Logger logger)
            throws IOException {
        super(in);
        this.logger = logger;
    }

    @Override
    public int read() throws IOException {
        logger.info("readByte()");
        return super.read();
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        logger.info("readByteBuffer({})", buf.remaining());
        return super.read(buf);
    }

    @Override
    public ByteBuffer read(ByteBufferPool bufferPool, int maxLength,
            EnumSet<ReadOption> opts) throws IOException,
            UnsupportedOperationException {
        logger.info("readByteBufferPool({},{},{})", bufferPool, maxLength, opts);
        return super.read(bufferPool, maxLength, opts);
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        logger.info("readByteArray({},{},{})", position, offset, length);
        return super.read(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        logger.info("readFullyByteArray({},{})", position, buffer.length);
        super.readFully(position, buffer);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        logger.info("readFullyByteArray({},{},{})", position, offset, length);
        super.readFully(position, buffer, offset, length);
    }

    @Override
    public FileDescriptor getFileDescriptor() throws IOException {
        logger.debug("getFileDescriptor()");
        return super.getFileDescriptor();
    }

    @Override
    public InputStream getWrappedStream() {
        logger.debug("getWrappedStream()");
        return super.getWrappedStream();
    }

}
