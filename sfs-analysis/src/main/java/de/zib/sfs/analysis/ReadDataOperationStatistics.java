/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    protected long localCount;

    protected String remoteHostname;

    public ReadDataOperationStatistics(String hostname, String className,
            String name, long startTime, long endTime, long data,
            String remoteHostname) {
        super(hostname, className, name, startTime, endTime, data);
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

        localCount = isLocal() ? 1L : 0L;
    }

    @Override
    public void add(OperationStatistics other) {
        if (!(other instanceof ReadDataOperationStatistics)) {
            throw new IllegalArgumentException(
                    "OperationStatistics types do not match: " + getClass()
                            + ", " + other.getClass());
        }
        localCount += ((ReadDataOperationStatistics) other).getLocalCount();

        super.add(other);
    }

    public long getLocalCount() {
        return localCount;
    }

    public void setLocalCount(long localCount) {
        this.localCount = localCount;
    }

    public String getRemoteHostname() {
        return remoteHostname;
    }

    public void setRemoteHostname(String remoteHostname) {
        this.remoteHostname = remoteHostname;
    }

    public boolean isLocal() {
        return "localhost".equals(remoteHostname)
                || hostname.equals(remoteHostname);
    }

    @Override
    public ReadDataOperationStatistics clone()
            throws CloneNotSupportedException {
        return new ReadDataOperationStatistics(hostname, className, name,
                startTime, endTime, data, remoteHostname);
    }

    @Override
    public int compareTo(OperationStatistics o) {
        int result = super.compareTo(o);
        if (result != 0) {
            return result;
        }

        ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) o;
        result = localCount < rdos.getLocalCount() ? -1 : localCount > rdos
                .getLocalCount() ? 1 : 0;
        if (result != 0) {
            return result;
        }

        result = remoteHostname.compareTo(rdos.getRemoteHostname());
        if (result != 0) {
            return result;
        }

        return 0;
    }

}
