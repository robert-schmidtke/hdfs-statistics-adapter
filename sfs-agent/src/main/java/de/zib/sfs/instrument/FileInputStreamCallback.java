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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileInputStreamCallback {

    private final FileInputStream fis;

    private Logger logger;

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
        logger = null;
    }

    public Logger getLogger() {
        return logger;
    }

    public long onOpenBegin(String name) {
        // don't monitor access to libraries and configurations in the JVM's
        // home
        if (name.startsWith(System.getProperty("java.home"))) {
            return -1L;
        }

        // check if log4j providers can be loaded already
        boolean hasProviders;
        try {
            ClassLoader classLoader = ClassLoader.getSystemClassLoader();
            if (classLoader != null) {
                hasProviders = classLoader.getResources(
                        "META-INF/log4j-provider.properties").hasMoreElements();
            } else {
                hasProviders = false;
            }
        } catch (Exception e) {
            hasProviders = false;
        }

        // we're too early in the JVM startup, don't initialize log4j yet
        if (!hasProviders) {
            return -1L;
        }

        // only log access to files that are not our own log file
        if (!name.equals(System.getProperty("de.zib.sfs.logFile.name"))) {
            logger = LogManager.getLogger("de.zib.sfs.AsyncLogger");
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onOpenEnd(long startTime, String name) {
        if (logger != null) {
            logger.info("{}:{}.open({}):void", System.currentTimeMillis()
                    - startTime, fis, name);
        }
    }

    public long onReadBegin() {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onReadEnd(long startTime, int readResult) {
        if (logger != null) {
            logger.info("{}:{}.read():{}->{}", System.currentTimeMillis()
                    - startTime, fis, readResult, "localhost");
        }
    }

    public long onReadBytesBegin(byte[] b, int off, int len) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onReadBytesEnd(long startTime, int readBytesResult, byte[] b,
            int off, int len) {
        if (logger != null) {
            logger.info("{}:{}.readBytes([{}],{},{}):{}->{}",
                    System.currentTimeMillis() - startTime, fis, b.length, off,
                    len, readBytesResult, "localhost");
        }
    }

}
