/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class ReadDataOperationInfo extends DataOperationInfo {

    protected final boolean local;

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
            long endTime, long data, boolean local) {
        super(hostname, name, startTime, endTime, data);
        this.local = local;
    }

    public boolean isLocal() {
        return local;
    }

}
