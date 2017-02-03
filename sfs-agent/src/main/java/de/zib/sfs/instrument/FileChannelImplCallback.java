/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.nio.ByteBuffer;

import sun.nio.ch.FileChannelImpl;

// FileChannelImpl is not supposed to be used
@SuppressWarnings("restriction")
public class FileChannelImplCallback {

    private final FileChannelImpl fci;

    public FileChannelImplCallback(FileChannelImpl fci, Object parent) {
        this.fci = fci;
    }

    public long onReadBegin(ByteBuffer dst) {
        return -1L;
    }

    public void onReadEnd(long startTime, int readResult, ByteBuffer dst) {
    }

    public long onReadBegin(ByteBuffer[] dsts, int offset, int length) {
        return -1L;
    }

    public void onReadEnd(long startTime, long readResult, ByteBuffer[] dsts,
            int offset, int length) {
    }

    public long onWriteBegin(ByteBuffer src) {
        return -1L;
    }

    public void onWriteEnd(long startTime, int writeResult, ByteBuffer src) {
    }

    public long onWriteBegin(ByteBuffer[] srcs, int offset, int length) {
        return -1L;
    }

    public void onWriteEnd(long startTime, long writeResult, ByteBuffer[] srcs,
            int offset, int length) {
    }

}
