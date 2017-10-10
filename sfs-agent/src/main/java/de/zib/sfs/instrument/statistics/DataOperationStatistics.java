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

public class DataOperationStatistics extends OperationStatistics {

    private long data;

    public DataOperationStatistics() {
    }

    public DataOperationStatistics(long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data) {
        this(1, startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data);
    }

    public DataOperationStatistics(long count, long timeBin, long cpuTime,
            OperationSource source, OperationCategory category, int fd,
            long data) {
        super(count, timeBin, cpuTime, source, category, fd);
        this.data = data;
    }

    public long getData() {
        return this.data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public DataOperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof DataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        OperationStatistics aggregate = super.aggregate(other);
        return new DataOperationStatistics(aggregate.getCount(),
                aggregate.getTimeBin(), aggregate.getCpuTime(),
                aggregate.getSource(), aggregate.getCategory(),
                aggregate.getFileDescriptor(),
                this.data + ((DataOperationStatistics) other).getData());
    }

    public static void getCsvHeaders(String separator, StringBuilder sb) {
        OperationStatistics.getCsvHeaders(separator, sb);
        sb.append(separator).append("data");
    }

    @Override
    public void toCsv(String separator, StringBuilder sb) {
        super.toCsv(separator, sb);
        sb.append(separator).append(this.data);
    }

    public static DataOperationStatistics fromCsv(String line, String separator,
            int off) {
        String[] values = line.split(separator);
        return new DataOperationStatistics(Long.parseLong(values[off + 0]),
                Long.parseLong(values[off + 1]),
                Long.parseLong(values[off + 2]),
                OperationSource.valueOf(values[off + 3].toUpperCase()),
                OperationCategory.valueOf(values[off + 4].toUpperCase()),
                Integer.parseInt(values[off + 5]),
                Long.parseLong(values[off + 6]));
    }

    @Override
    protected void toFlatBuffer(FlatBufferBuilder builder) {
        super.toFlatBuffer(builder);
        if (this.data > 0)
            OperationStatisticsFB.addData(builder, this.data);
    }

    public static DataOperationStatistics fromFlatBuffer(ByteBuffer buffer) {
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
        return new DataOperationStatistics(os.count(), os.timeBin(),
                os.cpuTime(), OperationSource.fromFlatBuffer(os.source()),
                OperationCategory.fromFlatBuffer(os.category()),
                os.fileDescriptor(), os.data());
    }

    @Override
    public void toByteBuffer(ByteBuffer hostname, int pid, ByteBuffer key,
            ByteBuffer bb) {
        OperationStatisticsBufferBuilder.serialize(hostname, pid, key, this,
                bb);
    }
}
