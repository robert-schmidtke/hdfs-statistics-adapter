/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileInputStream;

public class FileInputStreamCallback {

    private final FileInputStream fis;

    public FileInputStreamCallback(FileInputStream fis) {
        this.fis = fis;
    }

    public long onOpenBegin(String name) {
        return -1L;
    }

    public void onOpenEnd(long startTime, String name) {
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

}
