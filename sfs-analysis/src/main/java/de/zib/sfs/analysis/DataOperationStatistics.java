/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class DataOperationStatistics extends OperationStatistics {

    protected long data;

    public DataOperationStatistics() {
    }

    public DataOperationStatistics(String hostname, int pid, String className,
            String name, long startTime, long endTime, long data) {
        super(hostname, pid, className, name, startTime, endTime);
        this.data = data;
    }

    public DataOperationStatistics(DataOperationStatistics other) {
        super(other);
        setData(other.getData());
    }

    @Override
    public void add(OperationStatistics other, boolean strict) {
        if (!(other instanceof DataOperationStatistics)) {
            throw new IllegalArgumentException(
                    "OperationStatistics types do not match: " + getClass()
                            + ", " + other.getClass());
        }
        data += ((DataOperationStatistics) other).getData();

        super.add(other, strict);
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public DataOperationStatistics copy() {
        return new DataOperationStatistics(this);
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
