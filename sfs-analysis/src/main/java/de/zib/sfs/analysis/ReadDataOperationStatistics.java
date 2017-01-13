/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    public static class Aggregator extends DataOperationStatistics.Aggregator {

        private long localCount;

        public Aggregator() {
        }

        public Aggregator(ReadDataOperationStatistics statistics) {
            super(statistics);
            localCount += statistics.isLocal() ? 1 : 0;
        }

        public long getLocalCount() {
            return localCount;
        }

        public void setLocalCount(long localCount) {
            this.localCount = localCount;
        }

        @Override
        public void aggregate(OperationStatistics.Aggregator aggregator) {
            if (!(aggregator instanceof Aggregator)) {
                throw new IllegalArgumentException(
                        "aggregator must be of type " + getClass().getName());
            }
            super.aggregate(aggregator);

            localCount += ((Aggregator) aggregator).getLocalCount();
        }

        @Override
        public String getCsvHeaders(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.getCsvHeaders(separator));
            sb.append(separator).append("localCount");
            return sb.toString();
        }

        @Override
        public String toCsv(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toCsv(separator));
            sb.append(separator).append(localCount);
            return sb.toString();
        }
    }

    private String remoteHostname;

    public ReadDataOperationStatistics() {
    }

    public ReadDataOperationStatistics(String hostname, int pid,
            String className, String name, long startTime, long endTime,
            long data, String remoteHostname) {
        super(hostname, pid, className, name, startTime, endTime, data);
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

    public ReadDataOperationStatistics(ReadDataOperationStatistics other) {
        super(other);
        setRemoteHostname(other.getRemoteHostname());
    }

    public String getRemoteHostname() {
        return remoteHostname;
    }

    public void setRemoteHostname(String remoteHostname) {
        this.remoteHostname = remoteHostname;
    }

    public boolean isLocal() {
        return "localhost".equals(remoteHostname)
                || getHostname().equals(remoteHostname);
    }

    @Override
    public ReadDataOperationStatistics copy() {
        return new ReadDataOperationStatistics(this);
    }

    @Override
    public Aggregator getAggregator() {
        return new Aggregator(this);
    }

}
