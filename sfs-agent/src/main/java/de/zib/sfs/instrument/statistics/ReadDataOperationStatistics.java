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

        public Aggregator(long timeBinDuration, OperationSource source,
                OperationCategory category, long startTime, long endTime,
                long data, boolean isRemote) {
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

}
