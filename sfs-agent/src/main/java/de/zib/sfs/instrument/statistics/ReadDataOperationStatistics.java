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

public class ReadDataOperationStatistics extends DataOperationStatistics {

    private static final Queue<ReadDataOperationStatistics> pool = new ConcurrentLinkedQueue<>();

    private static final int REMOTE_COUNT_OFFSET = DataOperationStatistics.SIZE; // long
    private static final int REMOTE_CPU_TIME_OFFSET = REMOTE_COUNT_OFFSET + 8; // long
    private static final int REMOTE_DATA_OFFSET = REMOTE_CPU_TIME_OFFSET + 8; // long
    static final int SIZE = REMOTE_DATA_OFFSET + 8;

    public static ReadDataOperationStatistics getReadDataOperationStatistics() {
        ReadDataOperationStatistics rdos = pool.poll();
        if (rdos == null) {
            rdos = new ReadDataOperationStatistics();
        }
        return rdos;
    }

    public static ReadDataOperationStatistics getReadDataOperationStatistics(
            long count, long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd, long data, long remoteCount,
            long remoteCpuTime, long remoteData) {
        ReadDataOperationStatistics rdos = getReadDataOperationStatistics();
        getReadDataOperationStatistics(rdos, count, timeBin, cpuTime, source,
                category, fd, data, remoteCount, remoteCpuTime, remoteData);
        return rdos;
    }

    protected static void getReadDataOperationStatistics(
            ReadDataOperationStatistics rdos, long count, long timeBin,
            long cpuTime, OperationSource source, OperationCategory category,
            int fd, long data, long remoteCount, long remoteCpuTime,
            long remoteData) {
        DataOperationStatistics.getDataOperationStatistics(rdos, count, timeBin,
                cpuTime, source, category, fd, data);
        rdos.setRemoteCount(remoteCount);
        rdos.setRemoteCpuTime(remoteCpuTime);
        rdos.setRemoteData(remoteData);
    }

    public static ReadDataOperationStatistics getReadDataOperationStatistics(
            long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data, boolean isRemote) {
        return getReadDataOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data, isRemote ? 1 : 0,
                isRemote ? endTime - startTime : 0, isRemote ? data : 0);
    }

    @Override
    public void returnOperationStatistics() {
        pool.offer(this);
    }

    private ReadDataOperationStatistics() {
        this(SIZE);
    }

    protected ReadDataOperationStatistics(int size) {
        super(size);
    }

    public long getRemoteCount() {
        return this.bb.getLong(this.bb.position() + REMOTE_COUNT_OFFSET);
    }

    public void setRemoteCount(long remoteCount) {
        this.bb.putLong(this.bb.position() + REMOTE_COUNT_OFFSET, remoteCount);
    }

    public void incrementRemoteCount(long remoteCount) {
        long current = this.bb
                .getLong(this.bb.position() + REMOTE_COUNT_OFFSET);
        this.bb.putLong(this.bb.position() + REMOTE_COUNT_OFFSET,
                current + remoteCount);
    }

    public long getRemoteCpuTime() {
        return this.bb.getLong(this.bb.position() + REMOTE_CPU_TIME_OFFSET);
    }

    public void setRemoteCpuTime(long remoteCpuTime) {
        this.bb.putLong(this.bb.position() + REMOTE_CPU_TIME_OFFSET,
                remoteCpuTime);
    }

    public void incrementRemoteCpuTime(long remoteCpuTime) {
        long current = this.bb
                .getLong(this.bb.position() + REMOTE_CPU_TIME_OFFSET);
        this.bb.putLong(this.bb.position() + REMOTE_CPU_TIME_OFFSET,
                current + remoteCpuTime);
    }

    public long getRemoteData() {
        return this.bb.getLong(this.bb.position() + REMOTE_DATA_OFFSET);
    }

    public void setRemoteData(long remoteData) {
        this.bb.putLong(this.bb.position() + REMOTE_DATA_OFFSET, remoteData);
    }

    public void incrementRemoteData(long remoteData) {
        long current = this.bb.getLong(this.bb.position() + REMOTE_DATA_OFFSET);
        this.bb.putLong(this.bb.position() + REMOTE_DATA_OFFSET,
                current + remoteData);
    }

    @Override
    public ReadDataOperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof ReadDataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        super.aggregate(other);
        return this;
    }

    @Override
    public synchronized void doAggregation() {
        if (this.aggregate != null) {
            ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) this.aggregate;
            incrementRemoteCount(rdos.getRemoteCount());
            incrementRemoteCpuTime(rdos.getRemoteCpuTime());
            incrementRemoteData(rdos.getRemoteData());
            super.doAggregation();
        }
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
        sb.append(separator).append(getRemoteCount());
        sb.append(separator).append(getRemoteCpuTime());
        sb.append(separator).append(getRemoteData());
    }

    public static void fromCsv(String line, String separator, int off,
            ReadDataOperationStatistics rdos) {
        fromCsv(line.split(separator), off, rdos);
    }

    public static void fromCsv(String[] values, int off,
            ReadDataOperationStatistics rdos) {
        DataOperationStatistics.fromCsv(values, off, rdos);
        rdos.setRemoteCount(Long.parseLong(values[off + 7]));
        rdos.setRemoteCpuTime(Long.parseLong(values[off + 8]));
        rdos.setRemoteData(Long.parseLong(values[off + 9]));
    }

    @Override
    protected void toFlatBuffer(FlatBufferBuilder builder) {
        super.toFlatBuffer(builder);
        if (getRemoteCount() > 0)
            OperationStatisticsFB.addRemoteCount(builder, getRemoteCount());
        if (getRemoteCpuTime() > 0)
            OperationStatisticsFB.addRemoteCpuTime(builder, getRemoteCpuTime());
        if (getRemoteData() > 0)
            OperationStatisticsFB.addRemoteData(builder, getRemoteData());
    }

    public static void fromFlatBuffer(ByteBuffer buffer,
            ReadDataOperationStatistics rdos) {
        fromFlatBuffer(fromFlatBuffer(buffer), rdos);
    }

    protected static void fromFlatBuffer(OperationStatisticsFB osfb,
            ReadDataOperationStatistics rdos) {
        DataOperationStatistics.fromFlatBuffer(osfb, rdos);
        rdos.setRemoteCount(osfb.remoteCount());
        rdos.setRemoteCpuTime(osfb.remoteCpuTime());
        rdos.setRemoteData(osfb.remoteData());
    }

    @Override
    public void toByteBuffer(ByteBuffer hostname, int pid, ByteBuffer key,
            ByteBuffer bb) {
        OperationStatisticsBufferBuilder.serialize(hostname, pid, key, this,
                bb);
    }
}
