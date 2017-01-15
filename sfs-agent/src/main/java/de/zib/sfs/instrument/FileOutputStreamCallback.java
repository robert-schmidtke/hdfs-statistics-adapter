/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileOutputStreamCallback {

    private final FileOutputStream fos;

    private Logger logger;

    private static final Map<FileOutputStream, FileOutputStreamCallback> instances = new HashMap<>();

    public static FileOutputStreamCallback getInstance(FileOutputStream fos) {
        FileOutputStreamCallback instance = instances.get(fos);
        if (instance == null) {
            instance = new FileOutputStreamCallback(fos);
            instances.put(fos, instance);
        }
        return instance;
    }

    private FileOutputStreamCallback(FileOutputStream fos) {
        this.fos = fos;
        logger = null;
    }

    public Logger getLogger() {
        return logger;
    }

    public long onOpenBegin(String name, boolean append) {
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

    public void onOpenEnd(long startTime, String name, boolean append) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("{}-{}:{}.open({},{}):void", startTime, duration, fos,
                    name, append);
        }
    }

    public long onWriteBegin(int b, boolean append) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onWriteEnd(long startTime, int b, boolean append) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("{}-{}:{}.write({},{}):void", startTime, duration, fos,
                    b, append);
        }
    }

    public long onWriteBytesBegin(byte[] b, int off, int len, boolean append) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onWriteBytesEnd(long startTime, byte[] b, int off, int len,
            boolean append) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("{}-{}:{}.writeBytes([{}],{},{},{}):void", startTime,
                    duration, fos, b.length, off, len, append);
        }
    }

}
