/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import sun.nio.ch.FileChannelImpl;

// FileChannelImpl is not supposed to be used
@SuppressWarnings("restriction")
public class FileChannelImplCallback {

    private final FileChannelImpl fci;

    private Logger logger;

    private static final Map<FileChannelImpl, FileChannelImplCallback> instances = new HashMap<>();

    public static FileChannelImplCallback getInstance(FileChannelImpl fci,
            Object parent) {
        FileChannelImplCallback instance = instances.get(fci);
        if (instance == null) {
            instance = new FileChannelImplCallback(fci, parent);
            instances.put(fci, instance);
        }
        return instance;
    }

    private FileChannelImplCallback(FileChannelImpl fci, Object parent) {
        this.fci = fci;
        logger = null;
        if (parent instanceof FileInputStream) {
            logger = FileInputStreamCallback.getInstance(
                    (FileInputStream) parent).getLogger();
        } else if (parent instanceof FileOutputStream) {
            logger = FileOutputStreamCallback.getInstance(
                    (FileOutputStream) parent).getLogger();
        } else if (parent instanceof RandomAccessFile) {
            logger = RandomAccessFileCallback.getInstance(
                    (RandomAccessFile) parent).getLogger();
        }
    }

    public long onReadBegin(ByteBuffer dst) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onReadEnd(long startTime, int readResult, ByteBuffer dst) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.read({}):{}->{}", startTime, duration, fci,
            //        dst, readResult, "localhost");
        }
    }

    public long onReadBegin(ByteBuffer[] dsts, int offset, int length) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onReadEnd(long startTime, long readResult, ByteBuffer[] dsts,
            int offset, int length) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.read([{}],{},{}):{}->{}", startTime,
            //        duration, fci, dsts.length, offset, length, readResult,
            //        "localhost");
        }
    }

    public long onWriteBegin(ByteBuffer src) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onWriteEnd(long startTime, int writeResult, ByteBuffer src) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.write({}):{}", startTime, duration, fci, src,
            //        writeResult);
        }
    }

    public long onWriteBegin(ByteBuffer[] srcs, int offset, int length) {
        if (logger != null) {
            return System.currentTimeMillis();
        } else {
            return -1L;
        }
    }

    public void onWriteEnd(long startTime, long writeResult, ByteBuffer[] srcs,
            int offset, int length) {
        if (logger != null && startTime != -1L) {
            long duration = System.currentTimeMillis() - startTime;
            //logger.info("{}-{}:{}.write([{}],{},{}):{}", startTime, duration,
            //        fci, srcs.length, offset, length, writeResult);
        }
    }

}
