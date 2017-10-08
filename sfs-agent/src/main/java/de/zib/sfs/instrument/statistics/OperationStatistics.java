/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;

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

    private long count;

    private long timeBin, cpuTime;

    private OperationSource source;

    private OperationCategory category;

    private int fd;

    public OperationStatistics(long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd) {
        this(1, startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd);
    }

    public OperationStatistics(long count, long timeBin, long cpuTime,
            OperationSource source, OperationCategory category, int fd) {
        this.count = count;
        this.timeBin = timeBin;
        this.cpuTime = cpuTime;
        this.source = source;
        this.category = category;
        this.fd = fd;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getTimeBin() {
        return timeBin;
    }

    public void setTimeBin(long timeBin) {
        this.timeBin = timeBin;
    }

    public long getCpuTime() {
        return cpuTime;
    }

    public void setCpuTime(long cpuTime) {
        this.cpuTime = cpuTime;
    }

    public OperationSource getSource() {
        return source;
    }

    public void setSource(OperationSource source) {
        this.source = source;
    }

    public OperationCategory getCategory() {
        return category;
    }

    public void setCategory(OperationCategory category) {
        this.category = category;
    }

    public int getFileDescriptor() {
        return fd;
    }

    public void setFileDescriptor(int fd) {
        this.fd = fd;
    }

    public OperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (this == other) {
            throw new NotAggregatableException("Cannot aggregate self");
        }

        if (other.getTimeBin() != timeBin) {
            throw new NotAggregatableException("Time bins do not match: "
                    + timeBin + ", " + other.getTimeBin());
        }

        if (!other.getSource().equals(source)) {
            throw new NotAggregatableException("Sources do not match: " + source
                    + ", " + other.getSource());
        }

        if (!other.getCategory().equals(category)) {
            throw new NotAggregatableException("Categories do not match: "
                    + category + ", " + other.getCategory());
        }

        if (other.getFileDescriptor() != fd) {
            throw new NotAggregatableException("File descriptors do not match: "
                    + fd + ", " + other.getFileDescriptor());
        }

        return new OperationStatistics(count + other.getCount(), timeBin,
                cpuTime + other.getCpuTime(), source, category, fd);
    }

    public String getCsvHeaders(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append("count");
        sb.append(separator).append("timeBin");
        sb.append(separator).append("cpuTime");
        sb.append(separator).append("source");
        sb.append(separator).append("category");
        sb.append(separator).append("fileDescriptor");
        return sb.toString();
    }

    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(count);
        sb.append(separator).append(timeBin);
        sb.append(separator).append(cpuTime);
        sb.append(separator).append(source.name().toLowerCase());
        sb.append(separator).append(category.name().toLowerCase());
        sb.append(separator).append(fd);
        return sb.toString();
    }

    public static OperationStatistics fromCsv(String line, String separator,
            int off) {
        String[] values = line.split(separator);
        return new OperationStatistics(Long.parseLong(values[off + 0]),
                Long.parseLong(values[off + 1]),
                Long.parseLong(values[off + 2]),
                OperationSource.valueOf(values[off + 3].toUpperCase()),
                OperationCategory.valueOf(values[off + 4].toUpperCase()),
                Integer.parseInt(values[off + 5]));
    }

    public ByteBuffer toFlatBuffer(String hostname, int pid, String key) {
        FlatBufferBuilder builder = new FlatBufferBuilder(0);
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
        return builder.dataBuffer();
    }

    protected void toFlatBuffer(FlatBufferBuilder builder) {
        if (count > 0)
            OperationStatisticsFB.addCount(builder, count);
        if (timeBin > 0)
            OperationStatisticsFB.addTimeBin(builder, timeBin);
        if (cpuTime > 0)
            OperationStatisticsFB.addCpuTime(builder, cpuTime);
        OperationStatisticsFB.addSource(builder, source.toFlatBuffer());
        OperationStatisticsFB.addCategory(builder, category.toFlatBuffer());
        if (fd > 0)
            OperationStatisticsFB.addFileDescriptor(builder, fd);
    }

    public static OperationStatistics fromFlatBuffer(ByteBuffer buffer) {
        int length;
        if (buffer.remaining() < Constants.SIZE_PREFIX_LENGTH
                || (length = ByteBufferUtil.getSizePrefix(buffer)
                        + Constants.SIZE_PREFIX_LENGTH) > buffer.remaining()) {
            throw new BufferUnderflowException();
        }
        ByteBuffer osBuffer = ByteBufferUtil.removeSizePrefix(buffer);
        buffer.position(buffer.position() + length);

        OperationStatisticsFB os = OperationStatisticsFB
                .getRootAsOperationStatisticsFB(osBuffer);
        return new OperationStatistics(os.count(), os.timeBin(), os.cpuTime(),
                OperationSource.fromFlatBuffer(os.source()),
                OperationCategory.fromFlatBuffer(os.category()),
                os.fileDescriptor());
    }

    public ByteBuffer toByteBuffer(String hostname, int pid, String key) {
        return new OperationStatisticsBufferBuilder(this).serialize(hostname,
                pid, key);
    }

    public static OperationStatistics fromByteBuffer(ByteBuffer bb) {
        return new OperationStatisticsBufferBuilder(bb).deserialize();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append("{");
        sb.append(toCsv(",")).append("}");
        return sb.toString();
    }
}
