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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    // Shadow super class' LOG
    public static final Log LOG = LogFactory
            .getLog(WrappedFSDataInputStream.class);

    private static Map<String, String> HOSTNAME_CACHE = new HashMap<String, String>();

    public WrappedFSDataInputStream(FSDataInputStream in, Logger logger)
            throws IOException {
        this.in = in;
        this.logger = logger;
    }

    @Override
    public int read() throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read();
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.read():{}->{}", startTime, duration, this,
                result, getDatanodeHostNameString());
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b, off, len);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.read([{}],{},{}):{}->{}", startTime, duration,
                this, b.length, off, len, result, getDatanodeHostNameString());
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.read([{}]):{}->{}", startTime, duration, this,
                b.length, result, getDatanodeHostNameString());
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
        logger.info("{}-{}:{}.seek({}):void->{}", startTime, duration, this,
                desired, getDatanodeHostNameString());
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        long startTime = System.currentTimeMillis();
        boolean result = in.seekToNewSource(targetPos);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.seekToNewSource({}):{}->{}", startTime, duration,
                this, targetPos, result, getDatanodeHostNameString());
        return result;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(position, buffer, offset, length);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.read({},[{}],{},{}):{}->{}", startTime, duration,
                this, position, buffer.length, offset, length, result,
                getDatanodeHostNameString());
        return result;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.readFully({},[{}]):void->{}", startTime,
                duration, this, position, buffer.length,
                getDatanodeHostNameString());
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer, offset, length);
        long duration = System.currentTimeMillis() - startTime;
        logger.info("{}-{}:{}.readFully({},[{}],{},{}):void->{}", startTime,
                duration, this, position, buffer.length, offset, length,
                getDatanodeHostNameString());
    }

    // Helper methods

    /**
     * Gets the datanode that was last read from as a string. Should be called
     * after the first read operation has been performed.
     * 
     * @return "->" + hostname of the datanode, or empty string if the
     *         information is not available
     */
    private String getDatanodeHostNameString() {
        if (datanodeHostNameSupplier == null) {
            if (in instanceof HdfsDataInputStream) {
                // call Hadoop's method directly
                final HdfsDataInputStream hdfsIn = (HdfsDataInputStream) in;
                if (hdfsIn.getCurrentDatanode() != null) {
                    datanodeHostNameSupplier = () -> hdfsIn
                            .getCurrentDatanode().getHostName();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Using datanodeHostNameSupplier from Hadoop.");
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("datanodeHostNameSupplier from Hadoop has no DataNode information.");
                    }
                    datanodeHostNameSupplier = () -> "";
                }
            } else {
                try {
                    // Check if there's an appropriately named method available
                    // that returns the hostname of the current node that is
                    // being read from. Using the lambda factory provides almost
                    // direct invocation performance.
                    MethodHandles.Lookup methodHandlesLookup = MethodHandles
                            .lookup();

                    // try this stream or the one it wraps
                    Method getCurrentDatanodeHostNameMethod = null;
                    InputStream bindToStream = null;
                    try {
                        getCurrentDatanodeHostNameMethod = in
                                .getClass()
                                .getDeclaredMethod("getCurrentDatanodeHostName");
                        bindToStream = in;
                    } catch (NoSuchMethodException e) {
                        getCurrentDatanodeHostNameMethod = in
                                .getWrappedStream()
                                .getClass()
                                .getDeclaredMethod("getCurrentDatanodeHostName");
                        bindToStream = in.getWrappedStream();
                    }

                    MethodHandle datanodeHostNameSupplierTarget = LambdaMetafactory
                            .metafactory(
                                    methodHandlesLookup,
                                    "get",
                                    MethodType.methodType(Supplier.class,
                                            bindToStream.getClass()),
                                    MethodType.methodType(Object.class),
                                    methodHandlesLookup
                                            .unreflect(getCurrentDatanodeHostNameMethod),
                                    MethodType.methodType(Object.class))
                            .getTarget();
                    datanodeHostNameSupplier = (Supplier<String>) datanodeHostNameSupplierTarget
                            .bindTo(bindToStream).invoke();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Using 'getCurrentDatanodeHostName' as datanodeHostNameSupplier.");
                    }
                } catch (Throwable t) {
                    datanodeHostNameSupplier = () -> "";
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No datanodeHostNameSupplier available.", t);
                    }
                }
            }
        }

        // handle cases where we have to perform a reverse lookup if
        // hostname is an IP
        String hostname = datanodeHostNameSupplier.get();
        String cachedHostname = HOSTNAME_CACHE.get(hostname);
        if (cachedHostname == null) {
            try {
                // strip port if necessary
                int portIndex = hostname.indexOf(":");
                cachedHostname = InetAddress.getByName(
                        portIndex == -1 ? hostname : hostname.substring(0,
                                portIndex)).getHostName();
            } catch (UnknownHostException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not determine hostname for " + hostname, e);
                }
                cachedHostname = "";
            }
            HOSTNAME_CACHE.put(hostname, cachedHostname);
        }
        return cachedHostname;
    }

}
