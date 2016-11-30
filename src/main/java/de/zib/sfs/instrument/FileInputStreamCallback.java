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
    }

    public void onOpenBegin(String name) {
        System.out.println(name + " : " + ignoreFileName);
    }

    public void onOpenEnd() {
        System.out.println(ignoreFileName);
    }

    public void onReadBegin() {

    }

    public void onReadEnd() {

    }

    public void onReadBytesBegin(byte[] b, int off, int len) {

    }

    public void onReadBytesEnd() {

    }

}
