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
        this(1, startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, data);
    }

    public DataOperationStatistics(long count, long timeBin, long cpuTime,
            OperationSource source, OperationCategory category, long data) {
        super(count, timeBin, cpuTime, source, category);
        this.data = data;
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public DataOperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (!(other instanceof DataOperationStatistics)) {
            throw new OperationStatistics.NotAggregatableException(
                    "aggregator must be of type " + getClass().getName());
        }
        OperationStatistics aggregate = super.aggregate(other);
        return new DataOperationStatistics(aggregate.getCount(),
                aggregate.getTimeBin(), aggregate.getCpuTime(),
                aggregate.getSource(), aggregate.getCategory(),
                data + ((DataOperationStatistics) other).getData());
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

    public static DataOperationStatistics fromCsv(String line, String separator,
            int off) {
        String[] values = line.split(separator);
        return new DataOperationStatistics(Long.parseLong(values[off + 0]),
                Long.parseLong(values[off + 1]),
                Long.parseLong(values[off + 2]),
                OperationSource.valueOf(values[off + 3].toUpperCase()),
                OperationCategory.valueOf(values[off + 4].toUpperCase()),
                Long.parseLong(values[off + 5]));
    }
}
