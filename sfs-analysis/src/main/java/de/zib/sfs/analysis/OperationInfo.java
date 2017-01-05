/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class OperationInfo {

    protected final long startTime, endTime, duration;

    protected final String hostname, className, name;

    protected final OperationSource source;

    protected final OperationCategory category;

    public static class Aggregator {

        protected long count;

        protected long duration, minDuration, maxDuration;

        protected long minStartTime, maxEndTime;

        protected String hostname, className, name;

        protected OperationSource source;

        protected OperationCategory category;

        public Aggregator() {
            count = 0L;
            duration = 0L;
            minDuration = Long.MAX_VALUE;
            maxDuration = Long.MIN_VALUE;
            minStartTime = Long.MAX_VALUE;
            maxEndTime = Long.MIN_VALUE;
            hostname = null;
            className = null;
            name = null;
            source = null;
            category = null;
        }

        public void aggregate(OperationInfo operationInfo) {
            ++count;
            duration += operationInfo.getDuration();
            minDuration = Math.min(minDuration, operationInfo.getDuration());
            maxDuration = Math.max(maxDuration, operationInfo.getDuration());
            minStartTime = Math.min(minStartTime, operationInfo.getStartTime());
            maxEndTime = Math.max(maxEndTime, operationInfo.getEndTime());

            if (hostname == null) {
                hostname = operationInfo.getHostname();
            } else if (!hostname.equals(operationInfo.getHostname())) {
                throw new IllegalArgumentException("Hostnames do not match: "
                        + hostname + " vs. " + operationInfo.getHostname());
            }

            if (className == null) {
                className = operationInfo.getClassName();
            } else if (!className.equals(operationInfo.getClassName())) {
                throw new IllegalArgumentException("Class names do not match: "
                        + className + " vs. " + operationInfo.getClassName());
            }

            if (name == null) {
                name = operationInfo.getName();
            } else if (!name.equals(operationInfo.getName())) {
                throw new IllegalArgumentException("Names do not match: "
                        + name + " vs. " + operationInfo.getName());
            }

            if (source == null) {
                source = operationInfo.getSource();
            } else if (!source.equals(operationInfo.getSource())) {
                throw new IllegalArgumentException("Sources do not match: "
                        + source + " vs. " + operationInfo.getSource());
            }

            if (category == null) {
                category = operationInfo.getCategory();
            } else if (!category.equals(operationInfo.getCategory())) {
                throw new IllegalArgumentException("Categories do not match: "
                        + category + " vs. " + operationInfo.getCategory());
            }
        }

        public long getCount() {
            return count;
        }

        public String getHostname() {
            return hostname;
        }

        public String getClassName() {
            return className;
        }

        public String getName() {
            return name;
        }

        public long getMinStartTime() {
            return minStartTime;
        }

        public long getMaxEndTime() {
            return maxEndTime;
        }

        public long getDuration() {
            return duration;
        }

        public long getMinDuration() {
            return minDuration;
        }

        public long getMaxDuration() {
            return maxDuration;
        }

        public OperationSource getSource() {
            return source;
        }

        public OperationCategory getCategory() {
            return category;
        }

    }

    public OperationInfo(String hostname, String className, String name,
            long startTime, long endTime) {
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

    public String getHostname() {
        return hostname;
    }

    public String getClassName() {
        return className;
    }

    public String getName() {
        return name;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getDuration() {
        return duration;
    }

    public OperationSource getSource() {
        return source;
    }

    public OperationCategory getCategory() {
        return category;
    }

    public Aggregator getAggregator() {
        return new Aggregator();
    }
}
