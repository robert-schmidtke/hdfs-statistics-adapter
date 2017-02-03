/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.RandomAccessFile;

public class RandomAccessFileCallback {

    private final RandomAccessFile raf;

    public RandomAccessFileCallback(RandomAccessFile raf) {
        this.raf = raf;
    }

    public long onOpenBegin(String name, int mode) {
        return -1L;
    }

    public void onOpenEnd(long startTime, String name, int mode) {
    }

    public long onReadBegin() {
        return -1L;
    }

    public void onReadEnd(long startTime, int readResult) {
    }

    public long onReadBytesBegin(byte[] b, int off, int len) {
        return -1L;
    }

    public void onReadBytesEnd(long startTime, int readBytesResult, byte[] b,
            int off, int len) {
    }

    public long onWriteBegin(int b) {
        return -1L;
    }

    public void onWriteEnd(long startTime, int b) {
    }

    public long onWriteBytesBegin(byte[] b, int off, int len) {
        return -1L;
    }

    public void onWriteBytesEnd(long startTime, byte[] b, int off, int len) {
    }

}
