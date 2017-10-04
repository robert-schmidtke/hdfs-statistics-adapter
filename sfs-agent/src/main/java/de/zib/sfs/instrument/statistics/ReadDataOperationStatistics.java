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

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    private long remoteCount, remoteCpuTime, remoteData;

    public ReadDataOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, int fd, long data, boolean isRemote) {
        this(1, startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data, isRemote ? 1 : 0,
                isRemote ? endTime - startTime : 0, isRemote ? data : 0);
    }

    public ReadDataOperationStatistics(long count, long timeBin, long cpuTime,
            OperationSource source, OperationCategory category, int fd,
            long data, long remoteCount, long remoteCpuTime, long remoteData) {
        super(count, timeBin, cpuTime, source, category, fd, data);
        this.remoteCount = remoteCount;
        this.remoteCpuTime = remoteCpuTime;
        this.remoteData = remoteData;
    }

    public long getRemoteCount() {
        return remoteCount;
    }

    public void setRemoteCount(long remoteCount) {
        this.remoteCount = remoteCount;
    }

    public long getRemoteDuration() {
        return remoteCpuTime;
    }

    public void setRemoteDuration(long remoteDuration) {
        this.remoteCpuTime = remoteDuration;
    }

    public long getRemoteData() {
        return remoteData;
    }

    public void setRemoteData(long remoteData) {
        this.remoteData = remoteData;
    }

    @Override
    public ReadDataOperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof ReadDataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        DataOperationStatistics aggregate = super.aggregate(other);
        return new ReadDataOperationStatistics(aggregate.getCount(),
                aggregate.getTimeBin(), aggregate.getCpuTime(),
                aggregate.getSource(), aggregate.getCategory(),
                aggregate.getFileDescriptor(), aggregate.getData(),
                remoteCount + ((ReadDataOperationStatistics) other)
                        .getRemoteCount(),
                remoteCpuTime + ((ReadDataOperationStatistics) other)
                        .getRemoteDuration(),
                remoteData + ((ReadDataOperationStatistics) other)
                        .getRemoteData());
    }

    @Override
    public String getCsvHeaders(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getCsvHeaders(separator));
        sb.append(separator).append("remoteCount");
        sb.append(separator).append("remoteCpuTime");
        sb.append(separator).append("remoteData");
        return sb.toString();
    }

    @Override
    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toCsv(separator));
        sb.append(separator).append(remoteCount);
        sb.append(separator).append(remoteCpuTime);
        sb.append(separator).append(remoteData);
        return sb.toString();
    }

    public static ReadDataOperationStatistics fromCsv(String line,
            String separator, int off) {
        String[] values = line.split(separator);
        return new ReadDataOperationStatistics(Long.parseLong(values[off + 0]),
                Long.parseLong(values[off + 1]),
                Long.parseLong(values[off + 2]),
                OperationSource.valueOf(values[off + 3].toUpperCase()),
                OperationCategory.valueOf(values[off + 4].toUpperCase()),
                Integer.parseInt(values[off + 5]),
                Long.parseLong(values[off + 6]),
                Long.parseLong(values[off + 7]),
                Long.parseLong(values[off + 8]),
                Long.parseLong(values[off + 9]));
    }

    @Override
    protected void toByteBuffer(FlatBufferBuilder builder) {
        super.toByteBuffer(builder);
        OperationStatisticsFB.addRemoteCount(builder, remoteCount);
        OperationStatisticsFB.addRemoteCpuTime(builder, remoteCpuTime);
        OperationStatisticsFB.addRemoteData(builder, remoteData);
    }

    public static ReadDataOperationStatistics fromByteBuffer(
            ByteBuffer buffer) {
        int length;
        if (buffer.remaining() < 4
                || (length = OperationStatisticsFB.getSizePrefix(buffer)
                        + 4) > buffer.remaining()) {
            throw new BufferUnderflowException();
        }
        ByteBuffer osBuffer = buffer.slice();
        buffer.position(buffer.position() + length);

        OperationStatisticsFB os = OperationStatisticsFB
                .getSizePrefixedRootAsOperationStatisticsFB(osBuffer);
        return new ReadDataOperationStatistics(os.count(), os.timeBin(),
                os.cpuTime(), OperationSource.fromFlatBuffer(os.source()),
                OperationCategory.fromFlatBuffer(os.category()),
                os.fileDescriptor(), os.data(), os.remoteCount(),
                os.remoteCpuTime(), os.remoteData());
    }
}
