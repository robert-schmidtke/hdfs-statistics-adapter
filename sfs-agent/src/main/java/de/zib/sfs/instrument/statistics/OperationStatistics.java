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
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FlatBufferBuilder.ByteBufferFactory;

import de.zib.sfs.instrument.statistics.bb.OperationStatisticsBufferBuilder;
import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;

public class OperationStatistics {

    public static class NotAggregatableException extends Exception {
        private static final long serialVersionUID = 2284196048334825540L;

        public NotAggregatableException() {
            super();
        }

        public NotAggregatableException(String message) {
            super(message);
        }
    }

    protected final ByteBuffer bb;

    private static final int COUNT_OFFSET = 0; // long
    private static final int TIME_BIN_OFFSET = COUNT_OFFSET + 8; // long
    private static final int CPU_TIME_OFFSET = TIME_BIN_OFFSET + 8; // long
    private static final int SOURCE_OFFSET = CPU_TIME_OFFSET + 8; // byte
    private static final int CATEGORY_OFFSET = SOURCE_OFFSET + 1; // byte
    private static final int FILE_DESCRIPTOR_OFFSET = CATEGORY_OFFSET + 1; // int
    protected static final int SIZE = FILE_DESCRIPTOR_OFFSET + 4;

    protected OperationStatistics aggregate;

    private static ByteBufferFactory overflowByteBufferFactory;

    private static final Queue<OperationStatistics> pool = new ConcurrentLinkedQueue<>();

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

    private OperationStatistics() {
        this(SIZE);
    }

    protected OperationStatistics(int size) {
        // this.bb = ByteBuffer.allocate(size);
        this.bb = ByteBuffer.allocateDirect(size);
    }

    public long getCount() {
        return this.bb.getLong(this.bb.position() + COUNT_OFFSET);
    }

    public void setCount(long count) {
        this.bb.putLong(this.bb.position() + COUNT_OFFSET, count);
    }

    public void incrementCount(long count) {
        long current = this.bb.getLong(this.bb.position() + COUNT_OFFSET);
        this.bb.putLong(this.bb.position() + COUNT_OFFSET, current + count);
    }

    public long getTimeBin() {
        return this.bb.getLong(this.bb.position() + TIME_BIN_OFFSET);
    }

    public void setTimeBin(long timeBin) {
        this.bb.putLong(this.bb.position() + TIME_BIN_OFFSET, timeBin);
    }

    public long getCpuTime() {
        return this.bb.getLong(this.bb.position() + CPU_TIME_OFFSET);
    }

    public void setCpuTime(long cpuTime) {
        this.bb.putLong(this.bb.position() + CPU_TIME_OFFSET, cpuTime);
    }

    public void incrementCpuTime(long cpuTime) {
        long current = this.bb.getLong(this.bb.position() + CPU_TIME_OFFSET);
        this.bb.putLong(this.bb.position() + CPU_TIME_OFFSET,
                current + cpuTime);
    }

    public OperationSource getSource() {
        return OperationSource.values()[this.bb
                .get(this.bb.position() + SOURCE_OFFSET)];
    }

    public void setSource(OperationSource source) {
        this.bb.put(this.bb.position() + SOURCE_OFFSET,
                (byte) source.ordinal());
    }

    public OperationCategory getCategory() {
        return OperationCategory.values()[this.bb
                .get(this.bb.position() + CATEGORY_OFFSET)];
    }

    public void setCategory(OperationCategory category) {
        this.bb.put(this.bb.position() + CATEGORY_OFFSET,
                (byte) category.ordinal());
    }

    public int getFileDescriptor() {
        return this.bb.getInt(this.bb.position() + FILE_DESCRIPTOR_OFFSET);
    }

    public void setFileDescriptor(int fd) {
        this.bb.putInt(this.bb.position() + FILE_DESCRIPTOR_OFFSET, fd);
    }

    public OperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (this == other) {
            throw new NotAggregatableException("Cannot aggregate self");
        }

        if (other.getTimeBin() != getTimeBin()) {
            throw new NotAggregatableException("Time bins do not match: "
                    + getTimeBin() + ", " + other.getTimeBin());
        }

        if (!other.getSource().equals(getSource())) {
            throw new NotAggregatableException("Sources do not match: "
                    + getSource() + ", " + other.getSource());
        }

        if (!other.getCategory().equals(getCategory())) {
            throw new NotAggregatableException("Categories do not match: "
                    + getCategory() + ", " + other.getCategory());
        }

        if (other.getFileDescriptor() != getFileDescriptor()) {
            throw new NotAggregatableException("File descriptors do not match: "
                    + getFileDescriptor() + ", " + other.getFileDescriptor());
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
            incrementCount(this.aggregate.getCount());
            incrementCpuTime(this.aggregate.getCpuTime());
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
        sb.append(getCount());
        sb.append(separator).append(getTimeBin());
        sb.append(separator).append(getCpuTime());
        sb.append(separator).append(getSource().name().toLowerCase());
        sb.append(separator).append(getCategory().name().toLowerCase());
        sb.append(separator).append(getFileDescriptor());
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
        if (getCount() > 0)
            OperationStatisticsFB.addCount(builder, getCount());
        if (getTimeBin() > 0)
            OperationStatisticsFB.addTimeBin(builder, getTimeBin());
        if (getCpuTime() > 0)
            OperationStatisticsFB.addCpuTime(builder, getCpuTime());
        OperationStatisticsFB.addSource(builder, getSource().toFlatBuffer());
        OperationStatisticsFB.addCategory(builder,
                getCategory().toFlatBuffer());
        if (getFileDescriptor() > 0)
            OperationStatisticsFB.addFileDescriptor(builder,
                    getFileDescriptor());
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
