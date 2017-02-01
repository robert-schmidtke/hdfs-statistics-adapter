/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RandomAccessFileCallback {

    private final RandomAccessFile raf;

    private Logger logger;

    private static final Map<RandomAccessFile, RandomAccessFileCallback> instances = new HashMap<>();

    public static RandomAccessFileCallback getInstance(RandomAccessFile raf) {
        RandomAccessFileCallback instance = instances.get(raf);
        if (instance == null) {
            instance = new RandomAccessFileCallback(raf);
            instances.put(raf, instance);
        }
        return instance;
    }

    private RandomAccessFileCallback(RandomAccessFile raf) {
        this.raf = raf;
        logger = null;
    }

    public Logger getLogger() {
        return logger;
    }

    public long onOpenBegin(String name, int mode) {
        return -1L;
        /*
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
        */
    }

    public void onOpenEnd(long startTime, String name, int mode) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.open({},{}):void", startTime, duration, raf,
            //        name, mode);
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
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.read():{}->{}", startTime, duration, raf,
            //        readResult, "localhost");
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
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.readBytes([{}],{},{}):{}->{}", startTime,
            //        duration, raf, b.length, off, len, readBytesResult,
            //        "localhost");
        }
    }

    public long onWriteBegin(int b) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onWriteEnd(long startTime, int b) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.write({}):void", startTime, duration, raf, b);
        }
    }

    public long onWriteBytesBegin(byte[] b, int off, int len) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onWriteBytesEnd(long startTime, byte[] b, int off, int len) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.writeBytes([{}],{},{}):void", startTime,
            //        duration, raf, b.length, off, len);
        }
    }

}
