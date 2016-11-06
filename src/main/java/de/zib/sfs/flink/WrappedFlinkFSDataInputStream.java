/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.flink;

import java.io.IOException;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

public class WrappedFlinkFSDataInputStream extends FSInputStream {

    private final FSDataInputStream in;

    private final FileSystem.Statistics statistics;

    public WrappedFlinkFSDataInputStream(FSDataInputStream in,
            FileSystem.Statistics statistics) {
        this.in = in;
        this.statistics = statistics;
    }

    @Override
    public synchronized int read() throws IOException {
        int data = in.read();
        if (data == -1) {
            return -1;
        }
        statistics.incrementBytesRead(1);
        return data;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public synchronized int read(byte[] buffer, int offset, int length)
            throws IOException {
        int bytesRead = in.read(buffer, offset, length);
        if (bytesRead == -1) {
            return -1;
        }
        statistics.incrementBytesRead(bytesRead);
        return bytesRead;
    }

    @Override
    public synchronized long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        in.seek(pos);
    }

    @Override
    public synchronized boolean seekToNewSource(long pos) throws IOException {
        return false;
    }

    public String getCurrentDatanodeHostName() {
        // TODO implement
        return "";
    }

}
