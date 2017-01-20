/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.statistics;

public class DataOperationStatistics extends OperationStatistics {

    public static class Aggregator extends OperationStatistics.Aggregator {

        private long data;

        public Aggregator() {
        }

        public Aggregator(DataOperationStatistics statistics,
                long timeBinDuration) {
            super(statistics, timeBinDuration);
            data += statistics.getData();
        }

        public long getData() {
            return data;
        }

        public void setData(long data) {
            this.data = data;
        }

        @Override
        public Aggregator aggregate(OperationStatistics.Aggregator aggregator)
                throws NotAggregatableException {
            if (!(aggregator instanceof Aggregator)) {
                throw new OperationStatistics.Aggregator.NotAggregatableException(
                        "aggregator must be of type " + getClass().getName());
            }
            super.aggregate(aggregator);

            data += ((Aggregator) aggregator).getData();

            return this;
        }

        @Override
        public String getCsvHeaders(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.getCsvHeaders(separator));
            sb.append(separator).append("data");
            return sb.toString();
        }

        @Override
        public String toCsv(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.toCsv(separator));
            sb.append(separator).append(data);
            return sb.toString();
        }
    }

    private long data;

    public DataOperationStatistics() {
    }

    public DataOperationStatistics(String hostname, int pid, String key,
            String className, String name, String instance, long startTime,
            long endTime, long data) {
        super(hostname, pid, key, className, name, instance, startTime, endTime);
        this.data = data;
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toCsv(separator));
        sb.append(separator).append("data:").append(getData());
        return sb.toString();
    }

    @Override
    public Aggregator getAggregator(long timeBinDuration) {
        return new Aggregator(this, timeBinDuration);
    }
}
