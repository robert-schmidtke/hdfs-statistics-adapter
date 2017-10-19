/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.bb.OperationStatisticsBufferBuilder;
import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;

public class DataOperationStatistics extends OperationStatistics {

    private static final int DATA_OFFSET = OperationStatistics.SIZE; // long
    protected static final int SIZE = DATA_OFFSET + 8;

    private static final Queue<DataOperationStatistics> pool = new ConcurrentLinkedQueue<>();

    public static DataOperationStatistics getDataOperationStatistics() {
        DataOperationStatistics dos = pool.poll();
        if (dos == null) {
            dos = new DataOperationStatistics();
        }
        return dos;
    }

    public static DataOperationStatistics getDataOperationStatistics(long count,
            long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd, long data) {
        DataOperationStatistics dos = getDataOperationStatistics();
        getDataOperationStatistics(dos, count, timeBin, cpuTime, source,
                category, fd, data);
        return dos;
    }

    protected static void getDataOperationStatistics(
            DataOperationStatistics dos, long count, long timeBin, long cpuTime,
            OperationSource source, OperationCategory category, int fd,
            long data) {
        OperationStatistics.getOperationStatistics(dos, count, timeBin, cpuTime,
                source, category, fd);
        dos.setData(data);
    }

    public static DataOperationStatistics getDataOperationStatistics(
            long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data) {
        return getDataOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data);
    }

    @Override
    public void returnOperationStatistics() {
        pool.offer(this);
    }

    private DataOperationStatistics() {
        this(SIZE);
    }

    protected DataOperationStatistics(int size) {
        super(size);
    }

    public long getData() {
        return this.bb.getLong(this.bb.position() + DATA_OFFSET);
    }

    public void setData(long data) {
        this.bb.putLong(this.bb.position() + DATA_OFFSET, data);
    }

    public void incrementData(long data) {
        long current = this.bb.getLong(this.bb.position() + DATA_OFFSET);
        this.bb.putLong(this.bb.position() + DATA_OFFSET, current + data);
    }

    @Override
    public DataOperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof DataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        super.aggregate(other);
        return this;
    }

    @Override
    public synchronized void doAggregation() {
        if (this.aggregate != null) {
            incrementData(((DataOperationStatistics) this.aggregate).getData());
            super.doAggregation();
        }
    }

    public static void getCsvHeaders(String separator, StringBuilder sb) {
        OperationStatistics.getCsvHeaders(separator, sb);
        sb.append(separator).append("data");
    }

    @Override
    public void toCsv(String separator, StringBuilder sb) {
        super.toCsv(separator, sb);
        sb.append(separator).append(getData());
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
        if (getData() > 0)
            OperationStatisticsFB.addData(builder, getData());
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
