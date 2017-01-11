/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class OperationStatistics implements Cloneable {

    private long count;

    protected long startTime, endTime, duration;

    protected String hostname, className, name;

    protected OperationSource source;

    protected OperationCategory category;

    public OperationStatistics() {
    }

    public OperationStatistics(String hostname, String className, String name,
            long startTime, long endTime) {
        count = 1L;
        this.hostname = hostname;
        this.className = className;
        this.name = name;
        this.startTime = startTime;
        this.endTime = endTime;
        duration = endTime - startTime;

        switch (className) {
        case "java.io.FileInputStream":
        case "java.io.FileOutputStream":
        case "java.io.RandomAccessFile":
        case "sun.nio.ch.FileChannelImpl":
            source = OperationSource.JVM;
            break;
        case "de.zib.sfs.StatisticsFileSystem":
        case "de.zib.sfs.WrappedFSDataInputStream":
        case "de.zib.sfs.WrappedFSDataOutputStream":
            source = OperationSource.SFS;
            break;
        default:
            throw new IllegalArgumentException(
                    "Could not determine source from class " + className);
        }

        switch (name) {
        case "read":
        case "readBytes":
        case "readFully":
            category = OperationCategory.READ;
            break;
        case "write":
        case "writeBytes":
            category = OperationCategory.WRITE;
            break;
        case "append":
        case "create":
        case "delete":
        case "getFileBlockLocations":
        case "getFileStatus":
        case "listStatus":
        case "mkdirs":
        case "open":
        case "rename":
        case "seek":
        case "seekToNewSource":
            category = OperationCategory.OTHER;
            break;
        default:
            throw new IllegalArgumentException(
                    "Could not determine category from operation " + name);
        }
    }

    public void add(OperationStatistics other) {
        if (!other.getHostname().equals(hostname)) {
            throw new IllegalArgumentException("Hostnames do not match: "
                    + hostname + ", " + other.getHostname());
        }

        if (!other.getClassName().equals(className)) {
            throw new IllegalArgumentException("ClassNames do not match: "
                    + className + ", " + other.getClassName());
        }

        if (!other.getName().equals(name)) {
            throw new IllegalArgumentException("Names do not match: " + name
                    + ", " + other.getName());
        }

        if (!other.getSource().equals(source)) {
            throw new IllegalArgumentException("Sources do not match: "
                    + source + ", " + other.getSource());
        }

        if (!other.getCategory().equals(category)) {
            throw new IllegalArgumentException("Categories do not match: "
                    + category + ", " + other.getCategory());
        }

        count += other.getCount();
        startTime = Math.min(startTime, other.startTime);
        endTime = Math.max(endTime, other.getEndTime());
        duration += other.getDuration();
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    @Override
    public OperationStatistics clone() throws CloneNotSupportedException {
        return new OperationStatistics(hostname, className, name, startTime,
                endTime);
    }
}
