/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

public class FileInputStreamCallback {

    private final FileInputStream fis;

    private final String ignoreFileName;

    private static final Map<FileInputStream, FileInputStreamCallback> instances = new HashMap<>();

    public static FileInputStreamCallback getInstance(FileInputStream fis) {
        FileInputStreamCallback instance = instances.get(fis);
        if (instance == null) {
            instance = new FileInputStreamCallback(fis);
            instances.put(fis, instance);
        }
        return instance;
    }

    private FileInputStreamCallback(FileInputStream fis) {
        this.fis = fis;
        ignoreFileName = System.getProperty("de.zib.sfs.logFile.name");
        System.out.println("Ignore file name: " + ignoreFileName);
    }

    public void onOpenBegin(String name) {
        System.out.println("{ open: " + name);
    }

    public void onOpenEnd() {
        System.out.println("} open");
    }

    public void onReadBegin() {
        System.out.println("{ read");
    }

    public void onReadEnd(int readResult) {
        System.out.println("} read: " + readResult);
    }

    public void onReadBytesBegin(byte[] b, int off, int len) {
        System.out.println("{ readBytes: [" + b.length + "], " + off + ", "
                + len);
    }

    public void onReadBytesEnd(int readBytesResult) {
        System.out.println("} readBytes: " + readBytesResult);
    }

}
