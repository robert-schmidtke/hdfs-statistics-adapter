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

import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationStatisticsAggregator;

public class WrappedFSDataInputStream extends InputStream implements
        PositionedReadable, Seekable {

    private final FSDataInputStream in;

    private final String hostname;
    private Supplier<String> datanodeHostnameSupplier;

    private final OperationStatisticsAggregator aggregator;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory
            .getLog(WrappedFSDataInputStream.class);

    private static Map<String, String> HOSTNAME_CACHE = new HashMap<String, String>();

    public WrappedFSDataInputStream(FSDataInputStream in,
            OperationStatisticsAggregator aggregator) throws IOException {
        this.in = in;
        this.aggregator = aggregator;
        hostname = System.getProperty("de.zib.sfs.hostname");
    }

    @Override
    public int read() throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read();
        String datanodeHostname = getDatanodeHostNameString();
        aggregator.aggregate(new ReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime, System
                        .currentTimeMillis(), result == -1 ? 0 : 1,
                datanodeHostname, hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname)));
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b, off, len);
        String datanodeHostname = getDatanodeHostNameString();
        aggregator.aggregate(new ReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime, System
                        .currentTimeMillis(), result == -1 ? 0 : result,
                datanodeHostname, hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname)));
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(b);
        String datanodeHostname = getDatanodeHostNameString();
        aggregator.aggregate(new ReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime, System
                        .currentTimeMillis(), result == -1 ? 0 : result,
                datanodeHostname, hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname)));
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
        aggregator
                .aggregate(new OperationStatistics(OperationSource.JVM,
                        OperationCategory.OTHER, startTime, System
                                .currentTimeMillis()));
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        long startTime = System.currentTimeMillis();
        boolean result = in.seekToNewSource(targetPos);
        aggregator
                .aggregate(new OperationStatistics(OperationSource.JVM,
                        OperationCategory.OTHER, startTime, System
                                .currentTimeMillis()));
        return result;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        int result = in.read(position, buffer, offset, length);
        String datanodeHostname = getDatanodeHostNameString();
        aggregator.aggregate(new ReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime, System
                        .currentTimeMillis(), result == -1 ? 0 : result,
                datanodeHostname, hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname)));
        return result;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer);
        String datanodeHostname = getDatanodeHostNameString();
        aggregator.aggregate(new ReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime, System
                        .currentTimeMillis(), buffer.length, datanodeHostname,
                hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname)));
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.currentTimeMillis();
        in.readFully(position, buffer, offset, length);
        String datanodeHostname = getDatanodeHostNameString();
        aggregator.aggregate(new ReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime, System
                        .currentTimeMillis(), length, datanodeHostname,
                hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname)));
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
        if (datanodeHostnameSupplier == null) {
            if (in instanceof HdfsDataInputStream) {
                // call Hadoop's method directly
                final HdfsDataInputStream hdfsIn = (HdfsDataInputStream) in;
                if (hdfsIn.getCurrentDatanode() != null) {
                    datanodeHostnameSupplier = () -> hdfsIn
                            .getCurrentDatanode().getHostName();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Using datanodeHostNameSupplier from Hadoop.");
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("datanodeHostNameSupplier from Hadoop has no DataNode information.");
                    }
                    datanodeHostnameSupplier = () -> "";
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
                    datanodeHostnameSupplier = (Supplier<String>) datanodeHostNameSupplierTarget
                            .bindTo(bindToStream).invoke();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Using 'getCurrentDatanodeHostName' as datanodeHostNameSupplier.");
                    }
                } catch (Throwable t) {
                    datanodeHostnameSupplier = () -> "";
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No datanodeHostNameSupplier available.", t);
                    }
                }
            }
        }

        // handle cases where we have to perform a reverse lookup if
        // hostname is an IP
        String hostname = datanodeHostnameSupplier.get();
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
