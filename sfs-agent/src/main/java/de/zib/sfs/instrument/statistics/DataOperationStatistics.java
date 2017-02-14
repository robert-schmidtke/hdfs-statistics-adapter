/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

public class DataOperationStatistics extends OperationStatistics {

    private long data;

    public DataOperationStatistics(long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime,
            long data) {
        super(timeBinDuration, source, category, startTime, endTime);
        this.data = data;
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public OperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof DataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        super.aggregate(other);

        data += ((DataOperationStatistics) other).getData();

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
