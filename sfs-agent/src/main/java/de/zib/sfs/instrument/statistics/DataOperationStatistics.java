/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.nio.ByteBuffer;

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

    public static void fromCsv(String line, String separator, int off,
            DataOperationStatistics dos) {
        fromCsv(line.split(separator), off, dos);
    }

    public static void fromCsv(String[] values, int off,
            DataOperationStatistics dos) {
        OperationStatistics.fromCsv(values, off, dos);
        dos.setData(Long.parseLong(values[off + 6]));
    }

    @Override
    protected void toFlatBuffer(FlatBufferBuilder builder) {
        super.toFlatBuffer(builder);
        if (this.data > 0)
            OperationStatisticsFB.addData(builder, this.data);
    }

    public static void fromFlatBuffer(ByteBuffer buffer,
            DataOperationStatistics dos) {
        fromFlatBuffer(fromFlatBuffer(buffer), dos);
    }

    protected static void fromFlatBuffer(OperationStatisticsFB osfb,
            DataOperationStatistics dos) {
        OperationStatistics.fromFlatBuffer(osfb, dos);
        dos.setData(osfb.data());
    }

    @Override
    public void toByteBuffer(ByteBuffer hostname, int pid, ByteBuffer key,
            ByteBuffer bb) {
        OperationStatisticsBufferBuilder.serialize(hostname, pid, key, this,
                bb);
    }
}
