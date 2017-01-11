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

    public DataOperationStatistics(String hostname, String className,
            String name, long startTime, long endTime, long data) {
        super(hostname, className, name, startTime, endTime);
        this.data = data;
    }

    @Override
    public void add(OperationStatistics other) {
        if (!(other instanceof DataOperationStatistics)) {
            throw new IllegalArgumentException(
                    "OperationStatistics types do not match: " + getClass()
                            + ", " + other.getClass());
        }
        data += ((DataOperationStatistics) other).getData();

        super.add(other);
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public DataOperationStatistics clone() throws CloneNotSupportedException {
        return new DataOperationStatistics(hostname, className, name,
                startTime, endTime, data);
    }
}
