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
        super.aggregate(other);
        return this;
    }

    @Override
    public synchronized void doAggregation() {
        if (this.aggregate != null) {
            ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) this.aggregate;
            this.remoteCount += rdos.getRemoteCount();
            this.remoteCpuTime += rdos.getRemoteCpuTime();
            this.remoteData += rdos.getRemoteData();
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
        sb.append(separator).append(this.remoteCount);
        sb.append(separator).append(this.remoteCpuTime);
        sb.append(separator).append(this.remoteData);
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
        if (this.remoteCount > 0)
            OperationStatisticsFB.addRemoteCount(builder, this.remoteCount);
        if (this.remoteCpuTime > 0)
            OperationStatisticsFB.addRemoteCpuTime(builder, this.remoteCpuTime);
        if (this.remoteData > 0)
            OperationStatisticsFB.addRemoteData(builder, this.remoteData);
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
