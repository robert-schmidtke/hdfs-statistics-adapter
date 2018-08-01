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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class WrappedFSDataInputStream extends InputStream
        implements PositionedReadable, Seekable {

    private final FSDataInputStream in;
    private final int fd;

    private final String hostname;
    private Supplier<String> datanodeHostnameSupplier;

    private final boolean skipOther;

    private final LiveOperationStatisticsAggregator aggregator;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory
            .getLog(WrappedFSDataInputStream.class);

    private static Map<String, String> HOSTNAME_CACHE = new HashMap<>();

    public WrappedFSDataInputStream(FSDataInputStream in, Path f,
            LiveOperationStatisticsAggregator aggregator, boolean skipOther) {
        this.in = in;
        this.aggregator = aggregator;
        this.fd = this.aggregator.registerFileDescriptor(f.toString());
        this.hostname = System.getProperty("de.zib.sfs.hostname");
        this.skipOther = skipOther;
    }

    @Override
    public int read() throws IOException {
        long startTime = System.nanoTime();
        int result = this.in.read();
        String datanodeHostname = getDatanodeHostNameString();
        this.aggregator.aggregateReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime,
                System.nanoTime(), this.fd, result == -1 ? 0 : 1,
                this.hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname));
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        long startTime = System.nanoTime();
        int result = this.in.read(b, off, len);
        String datanodeHostname = getDatanodeHostNameString();
        this.aggregator.aggregateReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime,
                System.nanoTime(), this.fd, result == -1 ? 0 : result,
                this.hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname));
        return result;
    }

    @Override
    public int read(byte[] b) throws IOException {
        long startTime = System.nanoTime();
        int result = this.in.read(b);
        String datanodeHostname = getDatanodeHostNameString();
        this.aggregator.aggregateReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime,
                System.nanoTime(), this.fd, result == -1 ? 0 : result,
                this.hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname));
        return result;
    }

    @Override
    public long getPos() throws IOException {
        return this.in.getPos();
    }

    @Override
    public void seek(long desired) throws IOException {
        long startTime = System.nanoTime();
        this.in.seek(desired);
        if (!this.skipOther) {
            this.aggregator.aggregateOperationStatistics(OperationSource.SFS,
                    OperationCategory.OTHER, startTime, System.nanoTime(),
                    this.fd);
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        long startTime = System.nanoTime();
        boolean result = this.in.seekToNewSource(targetPos);
        if (!this.skipOther) {
            this.aggregator.aggregateOperationStatistics(OperationSource.SFS,
                    OperationCategory.OTHER, startTime, System.nanoTime(),
                    this.fd);
        }
        return result;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.nanoTime();
        int result = this.in.read(position, buffer, offset, length);
        String datanodeHostname = getDatanodeHostNameString();
        this.aggregator.aggregateReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime,
                System.nanoTime(), this.fd, result == -1 ? 0 : result,
                this.hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname));
        return result;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        long startTime = System.nanoTime();
        this.in.readFully(position, buffer);
        String datanodeHostname = getDatanodeHostNameString();
        this.aggregator.aggregateReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime,
                System.nanoTime(), this.fd, buffer.length,
                this.hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname));
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        long startTime = System.nanoTime();
        this.in.readFully(position, buffer, offset, length);
        String datanodeHostname = getDatanodeHostNameString();
        this.aggregator.aggregateReadDataOperationStatistics(
                OperationSource.SFS, OperationCategory.READ, startTime,
                System.nanoTime(), this.fd, length,
                this.hostname.equals(datanodeHostname)
                        || "localhost".equals(datanodeHostname));
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
        if (this.datanodeHostnameSupplier == null) {
            if (this.in instanceof HdfsDataInputStream) {
                // call Hadoop's method directly
                final HdfsDataInputStream hdfsIn = (HdfsDataInputStream) this.in;
                if (hdfsIn.getCurrentDatanode() != null) {
                    this.datanodeHostnameSupplier = () -> hdfsIn
                            .getCurrentDatanode().getHostName();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Using datanodeHostNameSupplier from Hadoop.");
                    }
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "datanodeHostNameSupplier from Hadoop has no DataNode information.");
                    }
                    this.datanodeHostnameSupplier = () -> "";
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
                        getCurrentDatanodeHostNameMethod = this.in.getClass()
                                .getDeclaredMethod(
                                        "getCurrentDatanodeHostName");
                        bindToStream = this.in;
                    } catch (NoSuchMethodException e) {
                        getCurrentDatanodeHostNameMethod = this.in
                                .getWrappedStream().getClass()
                                .getDeclaredMethod(
                                        "getCurrentDatanodeHostName");
                        bindToStream = this.in.getWrappedStream();
                    }

                    MethodHandle datanodeHostNameSupplierTarget = LambdaMetafactory
                            .metafactory(methodHandlesLookup, "get",
                                    MethodType.methodType(Supplier.class,
                                            bindToStream.getClass()),
                                    MethodType.methodType(Object.class),
                                    methodHandlesLookup.unreflect(
                                            getCurrentDatanodeHostNameMethod),
                                    MethodType.methodType(Object.class))
                            .getTarget();
                    this.datanodeHostnameSupplier = (Supplier<String>) datanodeHostNameSupplierTarget
                            .bindTo(bindToStream).invoke();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Using 'getCurrentDatanodeHostName' as datanodeHostNameSupplier.");
                    }
                } catch (Throwable t) {
                    this.datanodeHostnameSupplier = () -> "";
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No datanodeHostNameSupplier available.", t);
                    }
                }
            }
        }

        // handle cases where we have to perform a reverse lookup if
        // hostname is an IP
        String dnHostname = this.datanodeHostnameSupplier.get();
        String cachedHostname = HOSTNAME_CACHE.get(dnHostname);
        if (cachedHostname == null) {
            try {
                // strip port if necessary
                int portIndex = dnHostname.indexOf(":");
                cachedHostname = InetAddress
                        .getByName(portIndex == -1 ? dnHostname
                                : dnHostname.substring(0, portIndex))
                        .getHostName();
            } catch (UnknownHostException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not determine hostname for " + dnHostname,
                            e);
                }
                cachedHostname = "";
            }
            HOSTNAME_CACHE.put(dnHostname, cachedHostname);
        }
        return cachedHostname;
    }

}
