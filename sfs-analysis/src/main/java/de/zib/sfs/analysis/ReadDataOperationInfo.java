/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class ReadDataOperationInfo extends DataOperationInfo {

    protected final String remoteHostname;

    public static class Aggregator extends DataOperationInfo.Aggregator {

        protected long localCount;

        public Aggregator() {
            localCount = 0L;
        }

        @Override
        public void aggregate(OperationInfo operationInfo) {
            if (operationInfo instanceof ReadDataOperationInfo) {
                super.aggregate(operationInfo);
                ReadDataOperationInfo readDataOperationInfo = (ReadDataOperationInfo) operationInfo;
                if (readDataOperationInfo.isLocal()) {
                    ++localCount;
                }
            } else {
                throw new RuntimeException("operationInfo is not a "
                        + ReadDataOperationInfo.class.getName());
            }
        }

        public long getLocalCount() {
            return localCount;
        }

    }

    public ReadDataOperationInfo(String hostname, String name, long startTime,
            long endTime, long data, String remoteHostname) {
        super(hostname, name, startTime, endTime, data);
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

    public boolean isLocal() {
        return "localhost".equals(remoteHostname)
                || hostname.equals(remoteHostname);
    }

    @Override
    public de.zib.sfs.analysis.OperationInfo.Aggregator getAggregator() {
        return new Aggregator();
    }

}
