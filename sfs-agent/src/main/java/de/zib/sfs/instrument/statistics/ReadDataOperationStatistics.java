/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    private long remoteCount, remoteDuration, remoteData;

    public ReadDataOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, long data, boolean isRemote) {
        super(timeBinDuration, source, category, startTime, endTime, data);
        if (isRemote) {
            remoteCount = 1;
            remoteDuration = endTime - startTime;
            remoteData = data;
        } else {
            remoteCount = 0;
            remoteDuration = 0;
            remoteData = 0;
        }
    }

    public long getRemoteCount() {
        return remoteCount;
    }

    public void setRemoteCount(long remoteCount) {
        this.remoteCount = remoteCount;
    }

    public long getRemoteDuration() {
        return remoteDuration;
    }

    public void setRemoteDuration(long remoteDuration) {
        this.remoteDuration = remoteDuration;
    }

    public long getRemoteData() {
        return remoteData;
    }

    public void setRemoteData(long remoteData) {
        this.remoteData = remoteData;
    }

    @Override
    public OperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof ReadDataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        super.aggregate(other);

        remoteCount += ((ReadDataOperationStatistics) other)
                .getRemoteCount();
        remoteDuration += ((ReadDataOperationStatistics) other)
                .getRemoteDuration();
        remoteData += ((ReadDataOperationStatistics) other)
                .getRemoteData();

        return this;
    }

    @Override
    public String getCsvHeaders(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getCsvHeaders(separator));
        sb.append(separator).append("remoteCount");
        sb.append(separator).append("remoteDuration");
        sb.append(separator).append("remoteData");
        return sb.toString();
    }

    @Override
    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toCsv(separator));
        sb.append(separator).append(remoteCount);
        sb.append(separator).append(remoteDuration);
        sb.append(separator).append(remoteData);
        return sb.toString();
    }
}
