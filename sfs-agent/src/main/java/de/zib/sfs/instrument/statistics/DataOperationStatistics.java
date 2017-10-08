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

public class DataOperationStatistics extends OperationStatistics {

    private long data;

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
        return data;
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
                data + ((DataOperationStatistics) other).getData());
    }

    @Override
    public String getCsvHeaders(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getCsvHeaders(separator));
        sb.append(separator).append("data");
        return sb.toString();
    }

    @Override
    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toCsv(separator));
        sb.append(separator).append(data);
        return sb.toString();
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
        if (data > 0)
            OperationStatisticsFB.addData(builder, data);
    }

    public static DataOperationStatistics fromFlatBuffer(ByteBuffer buffer) {
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
        return new DataOperationStatistics(os.count(), os.timeBin(),
                os.cpuTime(), OperationSource.fromFlatBuffer(os.source()),
                OperationCategory.fromFlatBuffer(os.category()),
                os.fileDescriptor(), os.data());
    }

    @Override
    public ByteBuffer toByteBuffer(String hostname, int pid, String key) {
        return new OperationStatisticsBufferBuilder(this).serialize(hostname,
                pid, key);
    }
}
