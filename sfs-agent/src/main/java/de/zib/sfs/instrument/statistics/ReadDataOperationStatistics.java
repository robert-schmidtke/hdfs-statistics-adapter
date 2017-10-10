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
import java.nio.ByteOrder;

import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.bb.OperationStatisticsBufferBuilder;
import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    private long remoteCount, remoteCpuTime, remoteData;

    public ReadDataOperationStatistics() {
    }

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
        return this.remoteCount;
    }

    public void setRemoteCount(long remoteCount) {
        this.remoteCount = remoteCount;
    }

    public long getRemoteCpuTime() {
        return this.remoteCpuTime;
    }

    public void setRemoteCpuTime(long remoteDuration) {
        this.remoteCpuTime = remoteDuration;
    }

    public long getRemoteData() {
        return this.remoteData;
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
                this.remoteCount + ((ReadDataOperationStatistics) other)
                        .getRemoteCount(),
                this.remoteCpuTime + ((ReadDataOperationStatistics) other)
                        .getRemoteCpuTime(),
                this.remoteData + ((ReadDataOperationStatistics) other)
                        .getRemoteData());
    }

    public static void getCsvHeaders(String separator, StringBuilder sb) {
        DataOperationStatistics.getCsvHeaders(separator, sb);
        sb.append(separator).append("remoteCount");
        sb.append(separator).append("remoteCpuTime");
        sb.append(separator).append("remoteData");
    }

    @Override
    public void toCsv(String separator, StringBuilder sb) {
        super.toCsv(separator, sb);
        sb.append(separator).append(this.remoteCount);
        sb.append(separator).append(this.remoteCpuTime);
        sb.append(separator).append(this.remoteData);
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
    protected void toFlatBuffer(FlatBufferBuilder builder) {
        super.toFlatBuffer(builder);
        if (this.remoteCount > 0)
            OperationStatisticsFB.addRemoteCount(builder, this.remoteCount);
        if (this.remoteCpuTime > 0)
            OperationStatisticsFB.addRemoteCpuTime(builder, this.remoteCpuTime);
        if (this.remoteData > 0)
            OperationStatisticsFB.addRemoteData(builder, this.remoteData);
    }

    public static ReadDataOperationStatistics fromFlatBuffer(
            ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int length = Constants.SIZE_PREFIX_LENGTH;
        if (buffer.remaining() < length)
            throw new BufferUnderflowException();
        length += ByteBufferUtil.getSizePrefix(buffer);
        if (buffer.remaining() < length)
            throw new BufferUnderflowException();
        ByteBuffer osBuffer = ByteBufferUtil.removeSizePrefix(buffer);

        OperationStatisticsFB os = OperationStatisticsFB
                .getRootAsOperationStatisticsFB(osBuffer);
        buffer.position(buffer.position() + length);
        return new ReadDataOperationStatistics(os.count(), os.timeBin(),
                os.cpuTime(), OperationSource.fromFlatBuffer(os.source()),
                OperationCategory.fromFlatBuffer(os.category()),
                os.fileDescriptor(), os.data(), os.remoteCount(),
                os.remoteCpuTime(), os.remoteData());
    }

    @Override
    public void toByteBuffer(ByteBuffer hostname, int pid, ByteBuffer key,
            ByteBuffer bb) {
        OperationStatisticsBufferBuilder.serialize(hostname, pid, key, this,
                bb);
    }
}
