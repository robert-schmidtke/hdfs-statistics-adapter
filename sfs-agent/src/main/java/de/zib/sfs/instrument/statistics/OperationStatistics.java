/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Queue;

import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FlatBufferBuilder.ByteBufferFactory;

import de.zib.sfs.instrument.statistics.bb.OperationStatisticsBufferBuilder;
import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;
import de.zib.sfs.instrument.util.ResourcePool;

public class OperationStatistics extends ResourcePool.PoolableResource {

    public static class NotAggregatableException extends Exception {
        private static final long serialVersionUID = 2284196048334825540L;

        public NotAggregatableException() {
            super();
        }

        public NotAggregatableException(String message) {
            super(message);
        }
    }

    private long count;

    private long timeBin, cpuTime;

    private OperationSource source;

    private OperationCategory category;

    private int fd;

    protected OperationStatistics aggregate;

    private static ByteBufferFactory overflowByteBufferFactory;

    private static final Queue<OperationStatistics> pool = new ResourcePool<>(
            new OperationStatistics());

    public static OperationStatistics getOperationStatistics() {
        OperationStatistics os = pool.poll();
        if (os == null) {
            os = new OperationStatistics();
        }
        return os;
    }

    public static OperationStatistics getOperationStatistics(long count,
            long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd) {
        OperationStatistics os = getOperationStatistics();
        getOperationStatistics(os, count, timeBin, cpuTime, source, category,
                fd);
        return os;
    }

    protected static void getOperationStatistics(OperationStatistics os,
            long count, long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd) {
        os.setCount(count);
        os.setTimeBin(timeBin);
        os.setCpuTime(cpuTime);
        os.setSource(source);
        os.setCategory(category);
        os.setFileDescriptor(fd);
    }

    public static OperationStatistics getOperationStatistics(
            long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd) {
        return getOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd);
    }

    public void returnOperationStatistics() {
        pool.offer(this);
    }

    protected OperationStatistics() {
    }

    public long getCount() {
        return this.count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getTimeBin() {
        return this.timeBin;
    }

    public void setTimeBin(long timeBin) {
        this.timeBin = timeBin;
    }

    public long getCpuTime() {
        return this.cpuTime;
    }

    public void setCpuTime(long cpuTime) {
        this.cpuTime = cpuTime;
    }

    public OperationSource getSource() {
        return this.source;
    }

    public void setSource(OperationSource source) {
        this.source = source;
    }

    public OperationCategory getCategory() {
        return this.category;
    }

    public void setCategory(OperationCategory category) {
        this.category = category;
    }

    public int getFileDescriptor() {
        return this.fd;
    }

    public void setFileDescriptor(int fd) {
        this.fd = fd;
    }

    public OperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (this == other) {
            throw new NotAggregatableException("Cannot aggregate self");
        }

        if (other.getTimeBin() != this.timeBin) {
            throw new NotAggregatableException("Time bins do not match: "
                    + this.timeBin + ", " + other.getTimeBin());
        }

        if (!other.getSource().equals(this.source)) {
            throw new NotAggregatableException("Sources do not match: "
                    + this.source + ", " + other.getSource());
        }

        if (!other.getCategory().equals(this.category)) {
            throw new NotAggregatableException("Categories do not match: "
                    + this.category + ", " + other.getCategory());
        }

        if (other.getFileDescriptor() != this.fd) {
            throw new NotAggregatableException("File descriptors do not match: "
                    + this.fd + ", " + other.getFileDescriptor());
        }

        // allow the same aggregate to be set multiple times
        synchronized (this) {
            if (this.aggregate != null && this.aggregate != other) {
                // finish previous aggregation
                doAggregation();
            }
            this.aggregate = other;
        }

        return this;
    }

    public synchronized void doAggregation() {
        if (this.aggregate != null) {
            this.count += this.aggregate.getCount();
            this.cpuTime += this.aggregate.getCpuTime();
            this.aggregate.returnOperationStatistics();
            this.aggregate = null;
        }
    }

    public static void getCsvHeaders(String separator, StringBuilder sb) {
        sb.append("count");
        sb.append(separator).append("timeBin");
        sb.append(separator).append("cpuTime");
        sb.append(separator).append("source");
        sb.append(separator).append("category");
        sb.append(separator).append("fileDescriptor");
    }

    public void toCsv(String separator, StringBuilder sb) {
        sb.append(this.count);
        sb.append(separator).append(this.timeBin);
        sb.append(separator).append(this.cpuTime);
        sb.append(separator).append(this.source.name().toLowerCase());
        sb.append(separator).append(this.category.name().toLowerCase());
        sb.append(separator).append(this.fd);
    }

    public static void fromCsv(String line, String separator, int off,
            OperationStatistics os) {
        fromCsv(line.split(separator), off, os);
    }

    public static void fromCsv(String[] values, int off,
            OperationStatistics os) {
        os.setCount(Long.parseLong(values[off + 0]));
        os.setTimeBin(Long.parseLong(values[off + 1]));
        os.setCpuTime(Long.parseLong(values[off + 2]));
        os.setSource(OperationSource.valueOf(values[off + 3].toUpperCase()));
        os.setCategory(
                OperationCategory.valueOf(values[off + 4].toUpperCase()));
        os.setFileDescriptor(Integer.parseInt(values[off + 5]));
    }

    public void toFlatBuffer(String hostname, int pid, String key,
            ByteBuffer bb) {
        if (overflowByteBufferFactory == null) {
            overflowByteBufferFactory = new ByteBufferFactory() {
                @Override
                public ByteBuffer newByteBuffer(int capacity) {
                    // signal to the caller that the ByteBuffer was not
                    // sufficiently large
                    throw new BufferOverflowException();
                }
            };
        }

        FlatBufferBuilder builder = new FlatBufferBuilder(bb,
                overflowByteBufferFactory);
        toFlatBuffer(builder, hostname, pid, key);
    }

    private void toFlatBuffer(FlatBufferBuilder builder, String hostname,
            int pid, String key) {
        int hostnameOffset = builder.createString(hostname);
        int keyOffset = builder.createString(key);
        OperationStatisticsFB.startOperationStatisticsFB(builder);
        OperationStatisticsFB.addHostname(builder, hostnameOffset);
        if (pid > 0)
            OperationStatisticsFB.addPid(builder, pid);
        OperationStatisticsFB.addKey(builder, keyOffset);
        toFlatBuffer(builder);
        int os = OperationStatisticsFB.endOperationStatisticsFB(builder);
        OperationStatisticsFB
                .finishSizePrefixedOperationStatisticsFBBuffer(builder, os);
    }

    protected void toFlatBuffer(FlatBufferBuilder builder) {
        if (this.count > 0)
            OperationStatisticsFB.addCount(builder, this.count);
        if (this.timeBin > 0)
            OperationStatisticsFB.addTimeBin(builder, this.timeBin);
        if (this.cpuTime > 0)
            OperationStatisticsFB.addCpuTime(builder, this.cpuTime);
        OperationStatisticsFB.addSource(builder, this.source.toFlatBuffer());
        OperationStatisticsFB.addCategory(builder,
                this.category.toFlatBuffer());
        if (this.fd > 0)
            OperationStatisticsFB.addFileDescriptor(builder, this.fd);
    }

    protected static OperationStatisticsFB fromFlatBuffer(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int length = Constants.SIZE_PREFIX_LENGTH;
        if (buffer.remaining() < length)
            throw new BufferUnderflowException();
        length += ByteBufferUtil.getSizePrefix(buffer);
        if (buffer.remaining() < length)
            throw new BufferUnderflowException();
        ByteBuffer osBuffer = ByteBufferUtil.removeSizePrefix(buffer);
        OperationStatisticsFB osfb = OperationStatisticsFB
                .getRootAsOperationStatisticsFB(osBuffer);
        buffer.position(buffer.position() + length);
        return osfb;
    }

    public static void fromFlatBuffer(ByteBuffer buffer,
            OperationStatistics os) {
        fromFlatBuffer(fromFlatBuffer(buffer), os);
    }

    protected static void fromFlatBuffer(OperationStatisticsFB osfb,
            OperationStatistics os) {
        os.setCount(osfb.count());
        os.setTimeBin(osfb.timeBin());
        os.setCpuTime(osfb.cpuTime());
        os.setSource(OperationSource.fromFlatBuffer(osfb.source()));
        os.setCategory(OperationCategory.fromFlatBuffer(osfb.category()));
        os.setFileDescriptor(osfb.fileDescriptor());
    }

    public void toByteBuffer(ByteBuffer hostname, int pid, ByteBuffer key,
            ByteBuffer bb) {
        OperationStatisticsBufferBuilder.serialize(hostname, pid, key, this,
                bb);
    }

    public static void fromByteBuffer(ByteBuffer bb, OperationStatistics os) {
        OperationStatisticsBufferBuilder.deserialize(bb, os);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append("{");
        toCsv(",", sb);
        sb.append("}");
        return sb.toString();
    }
}
