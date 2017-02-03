/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileOutputStream;

public class FileOutputStreamCallback {

    private final FileOutputStream fos;

    public FileOutputStreamCallback(FileOutputStream fos) {
        this.fos = fos;
    }

    public long onOpenBegin(String name, boolean append) {
        return -1L;
    }

    public void onOpenEnd(long startTime, String name, boolean append) {
    }

    public long onWriteBegin(int b, boolean append) {
        return -1L;
    }

    public void onWriteEnd(long startTime, int b, boolean append) {
    }

    public long onWriteBytesBegin(byte[] b, int off, int len, boolean append) {
        return -1L;
    }

    public void onWriteBytesEnd(long startTime, byte[] b, int off, int len,
            boolean append) {
    }

}
