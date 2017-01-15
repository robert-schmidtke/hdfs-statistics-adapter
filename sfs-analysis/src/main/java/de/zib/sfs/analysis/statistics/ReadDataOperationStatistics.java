/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.statistics;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    public static class Aggregator extends DataOperationStatistics.Aggregator {

        private long remoteCount, remoteData;

        public Aggregator() {
        }

        public Aggregator(ReadDataOperationStatistics statistics) {
            super(statistics);
            if (statistics.isRemote()) {
                remoteCount = 1;
                remoteData = statistics.getData();
            } else {
                remoteCount = 0;
                remoteData = 0;
            }
        }

        public long getRemoteCount() {
            return remoteCount;
        }

        public void setRemoteCount(long remoteCount) {
            this.remoteCount = remoteCount;
        }

        public long getRemoteData() {
            return remoteData;
        }

        public void setRemoteData(long remoteData) {
            this.remoteData = remoteData;
        }

        @Override
        public void aggregate(OperationStatistics.Aggregator aggregator)
                throws NotAggregatableException {
            if (!(aggregator instanceof Aggregator)) {
                throw new OperationStatistics.Aggregator.NotAggregatableException(
                        "aggregator must be of type " + getClass().getName());
            }
            super.aggregate(aggregator);

            remoteCount += ((Aggregator) aggregator).getRemoteCount();
            remoteData += ((Aggregator) aggregator).getRemoteData();
        }

        @Override
        public String getCsvHeaders(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.getCsvHeaders(separator));
            sb.append(separator).append("remoteCount");
            sb.append(separator).append("remoteData");
            return sb.toString();
        }

        @Override
        public String toCsv(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toCsv(separator));
            sb.append(separator).append(remoteCount);
            sb.append(separator).append(remoteData);
            return sb.toString();
        }
    }

    private String remoteHostname;

    public ReadDataOperationStatistics() {
    }

    public ReadDataOperationStatistics(String hostname, int pid,
            String className, String name, String instance, long startTime,
            long endTime, long data, String remoteHostname) {
        super(hostname, pid, className, name, instance, startTime, endTime,
                data);
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
    }

    public String getRemoteHostname() {
        return remoteHostname;
    }

    public void setRemoteHostname(String remoteHostname) {
        this.remoteHostname = remoteHostname;
    }

    public boolean isRemote() {
        return !("localhost".equals(remoteHostname) || getHostname().equals(
                remoteHostname));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append("{");
        sb.append("pid:").append(getPid());
        sb.append(",hostname:").append(getHostname());
        sb.append(",className:").append(getClassName());
        sb.append(",name:").append(getName());
        sb.append(",instance:").append(getInstance());
        sb.append(",startTime:").append(getStartTime());
        sb.append(",endTime:").append(getEndTime());
        sb.append(",duration:").append(getDuration());
        sb.append(",data:").append(getData());
        sb.append(",remoteHostname:").append(getRemoteHostname());
        sb.append("}");
        return sb.toString();
    }

    @Override
    public Aggregator getAggregator() {
        return new Aggregator(this);
    }

}
