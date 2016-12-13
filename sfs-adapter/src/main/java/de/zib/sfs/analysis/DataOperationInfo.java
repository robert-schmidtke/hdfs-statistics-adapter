/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class DataOperationInfo extends OperationInfo {

    protected final long data;

    public static class Aggregator extends OperationInfo.Aggregator {

        protected long data, minData, maxData;

        public Aggregator() {
            data = 0L;
            minData = Long.MAX_VALUE;
            maxData = Long.MIN_VALUE;
        }

        @Override
        public void aggregate(OperationInfo operationInfo) {
            if (operationInfo instanceof DataOperationInfo) {
                super.aggregate(operationInfo);
                DataOperationInfo dataOperationInfo = (DataOperationInfo) operationInfo;
                data += dataOperationInfo.getData();
                minData = Math.min(minData, dataOperationInfo.getData());
                maxData = Math.max(maxData, dataOperationInfo.getData());
            } else {
                throw new RuntimeException("operationInfo is not a "
                        + DataOperationInfo.class.getName());
            }
        }

        public long getData() {
            return data;
        }

        public long getMinData() {
            return minData;
        }

        public long getMaxData() {
            return maxData;
        }

    }

    public DataOperationInfo(String hostname, String name, long startTime,
            long endTime, long data) {
        super(hostname, name, startTime, endTime);
        this.data = data;
    }

    public long getData() {
        return data;
    }

    @Override
    public de.zib.sfs.analysis.OperationInfo.Aggregator getAggregator() {
        return new Aggregator();
    }
}
