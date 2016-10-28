/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.function.Supplier;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.logging.log4j.Logger;

public class WrappedFSDataInputStream extends InputStream implements
        PositionedReadable, Seekable {

    private final FSDataInputStream in;

    private Supplier<String> datanodeHostNameSupplier;

    private final Logger logger;

    public WrappedFSDataInputStream(FSDataInputStream in, Logger logger)
            throws IOException {
        this.in = in;
        this.logger = logger;

        if (in instanceof HdfsDataInputStream) {
            // call Hadoop's method directly
            datanodeHostNameSupplier = () -> ((HdfsDataInputStream) in)
                    .getCurrentDatanode().getHostName();
        } else {
            try {
                // Check if there's an appropriately named method available that
                // returns the hostname of the current node that is being read
                // from. Using the lambda factory provides almost direct
                // invocation performance.
                MethodHandles.Lookup methodHandlesLookup = MethodHandles
                        .lookup();
                Method getCurrentDatanodeHostNameMethod = in.getClass()
                        .getDeclaredMethod("getCurrentDatanodeHostName");
                MethodHandle getCurrentDatanodeHostNameMethodHandle = methodHandlesLookup
                        .unreflect(getCurrentDatanodeHostNameMethod);
                datanodeHostNameSupplier = (Supplier<String>) LambdaMetafactory
                        .metafactory(MethodHandles.lookup(),
                                "getCurrentDatanodeHostName",
                                MethodType.methodType(Supplier.class),
                                getCurrentDatanodeHostNameMethodHandle.type(),
                                getCurrentDatanodeHostNameMethodHandle,
                                getCurrentDatanodeHostNameMethodHandle.type())
                        .getTarget().invokeExact();
            } catch (Throwable t) {
                datanodeHostNameSupplier = () -> "";
            }
        }
    }

    @Override
    public int read() throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read();
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read():{}", duration, this,
                datanodeHostNameSupplier.get(), result);
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b, off, len);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read([{}],{},{}):{}", duration, this,
                datanodeHostNameSupplier.get(), b.length, off, len, result);
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read([{}]):{}", duration, this,
                datanodeHostNameSupplier.get(), b.length, result);
        return result;
    }

    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    @Override
    public void seek(long desired) throws IOException {
        long startTime = System.currentTimeMillis();
        in.seek(desired);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.seek({}):void", duration, this,
                datanodeHostNameSupplier.get(), desired);
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        long startTime = System.currentTimeMillis();
        boolean result = in.seekToNewSource(targetPos);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.seekToNewSource({}):{}", duration, this,
                datanodeHostNameSupplier.get(), targetPos, result);
        return result;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(position, buffer, offset, length);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.read({},[{}],{},{}):{}", duration, this,
                datanodeHostNameSupplier.get(), position, buffer.length,
                offset, length, result);
        return result;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.readFully({},[{}]):void", duration, this,
                datanodeHostNameSupplier.get(), position, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer, offset, length);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}:{}{}.readFully({},[{}],{},{}):void", duration, this,
                datanodeHostNameSupplier.get(), position, buffer.length,
                offset, length);
    }

}
