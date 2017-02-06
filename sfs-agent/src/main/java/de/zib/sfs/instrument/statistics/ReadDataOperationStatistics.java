/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    public static class Aggregator extends DataOperationStatistics.Aggregator {

        private long remoteCount, remoteDuration, remoteData;

        public Aggregator() {
        }

        public Aggregator(ReadDataOperationStatistics statistics,
                long timeBinDuration) {
            super(statistics, timeBinDuration);
            if (statistics.isRemote()) {
                remoteCount = 1;
                remoteDuration = statistics.getDuration();
                remoteData = statistics.getData();
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
        public Aggregator aggregate(OperationStatistics.Aggregator aggregator)
                throws NotAggregatableException {
            if (!(aggregator instanceof Aggregator)) {
                throw new OperationStatistics.Aggregator.NotAggregatableException(
                        "aggregator must be of type " + getClass().getName());
            }
            super.aggregate(aggregator);

            remoteCount += ((Aggregator) aggregator).getRemoteCount();
            remoteDuration += ((Aggregator) aggregator).getRemoteDuration();
            remoteData += ((Aggregator) aggregator).getRemoteData();

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

    private String remoteHostname;

    private boolean remote;

    public ReadDataOperationStatistics() {
    }

    public ReadDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime,
            long data, String remoteHostname, boolean remote) {
        super(source, category, startTime, endTime, data);
        if (remoteHostname == null) {
            this.remoteHostname = null;
        } else {
            // hostname is usually obtained via $(hostname), remoteHostname
            // could be a reverse DNS lookup, so it could have a domain
            // attached, e.g. "acme" vs. "acme.example.com"
            int index = remoteHostname.indexOf(".");
            if (index != -1) {
                this.remoteHostname = remoteHostname.substring(0, index);
            } else {
                this.remoteHostname = remoteHostname;
            }
        }
        this.remote = remote;
    }

    public String getRemoteHostname() {
        return remoteHostname;
    }

    public void setRemoteHostname(String remoteHostname) {
        this.remoteHostname = remoteHostname;
    }

    public boolean isRemote() {
        return remote;
    }

    public void setRemote(boolean remote) {
        this.remote = remote;
    }

    @Override
    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toCsv(separator));
        sb.append(separator).append("remoteHostname:").append(remoteHostname);
        return sb.toString();
    }

    @Override
    public Aggregator getAggregator(long timeBinDuration) {
        return new Aggregator(this, timeBinDuration);
    }

}
