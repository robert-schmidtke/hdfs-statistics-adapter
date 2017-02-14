/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

public class OperationStatistics {

    public static class NotAggregatableException extends Exception {
        private static final long serialVersionUID = 2284196048334825540L;

        public NotAggregatableException() {
            super();
        }

        public NotAggregatableException(String message) {
            super(message);
        }
    }

    private long count;

    private long timeBin, cpuTime;

    private OperationSource source;

    private OperationCategory category;

    public OperationStatistics(long timeBinDuration, OperationSource source,
            OperationCategory category, long startTime, long endTime) {
        count = 1;
        timeBin = startTime - startTime % timeBinDuration;
        cpuTime = endTime - startTime;
        this.source = source;
        this.category = category;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getTimeBin() {
        return timeBin;
    }

    public void setTimeBin(long timeBin) {
        this.timeBin = timeBin;
    }

    public long getCpuTime() {
        return cpuTime;
    }

    public void setCpuTime(long cpuTime) {
        this.cpuTime = cpuTime;
    }

    public OperationSource getSource() {
        return source;
    }

    public void setSource(OperationSource source) {
        this.source = source;
    }

    public OperationCategory getCategory() {
        return category;
    }

    public void setCategory(OperationCategory category) {
        this.category = category;
    }

    public OperationStatistics aggregate(OperationStatistics other)
            throws NotAggregatableException {
        if (this == other) {
            throw new NotAggregatableException("Cannot aggregate self");
        }

        if (other.getTimeBin() != timeBin) {
            throw new NotAggregatableException("Time bins do not match: "
                    + timeBin + ", " + other.getTimeBin());
        }

        if (!other.getSource().equals(source)) {
            throw new NotAggregatableException("Sources do not match: " + source
                    + ", " + other.getSource());
        }

        if (!other.getCategory().equals(category)) {
            throw new NotAggregatableException("Categories do not match: "
                    + category + ", " + other.getCategory());
        }

        count += other.getCount();
        cpuTime += other.getCpuTime();

        return this;
    }

    public String getCsvHeaders(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append("count");
        sb.append(separator).append("timeBin");
        sb.append(separator).append("cpuTime");
        sb.append(separator).append("source");
        sb.append(separator).append("category");
        return sb.toString();
    }

    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append(count);
        sb.append(separator).append(timeBin);
        sb.append(separator).append(cpuTime);
        sb.append(separator).append(source.name().toLowerCase());
        sb.append(separator).append(category.name().toLowerCase());
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append("{");
        sb.append(toCsv(",")).append("}");
        return sb.toString();
    }
}
