/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

public class OperationStatistics {

    public static class Aggregator {

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

        private String hostname;

        private long startTime, endTime, duration;

        private OperationSource source;

        private OperationCategory category;

        public Aggregator() {
        }

        public Aggregator(OperationStatistics statistics) {
            count = 1;
            hostname = statistics.getHostname();
            startTime = statistics.getStartTime();
            endTime = statistics.getEndTime();
            duration = statistics.getDuration();

            switch (statistics.getClassName()) {
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
                        "Could not determine source from class "
                                + statistics.getClassName());
            }

            switch (statistics.getName()) {
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
                        "Could not determine category from operation "
                                + statistics.getName());
            }
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
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

        public void aggregate(Aggregator aggregator)
                throws NotAggregatableException {
            if (!aggregator.getHostname().equals(hostname)) {
                throw new NotAggregatableException("Hostnames do not match: "
                        + hostname + ", " + aggregator.getHostname());
            }

            if (!aggregator.getSource().equals(source)) {
                throw new NotAggregatableException("Sources do not match: "
                        + source + ", " + aggregator.getSource());
            }

            if (!aggregator.getCategory().equals(category)) {
                throw new NotAggregatableException("Categories do not match: "
                        + category + ", " + aggregator.getCategory());
            }

            ++count;
            startTime = Math.min(startTime, aggregator.getStartTime());
            endTime = Math.max(endTime, aggregator.getEndTime());
            duration += aggregator.getDuration();
        }

        public String getCsvHeaders(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append("hostname");
            sb.append(separator).append("source");
            sb.append(separator).append("category");
            sb.append(separator).append("startTime");
            sb.append(separator).append("endTime");
            sb.append(separator).append("duration");
            return sb.toString();
        }

        public String toCsv(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(hostname);
            sb.append(separator).append(source);
            sb.append(separator).append(category);
            sb.append(separator).append(startTime);
            sb.append(separator).append(endTime);
            sb.append(separator).append(duration);
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

    private long startTime, endTime;

    private String hostname, className, name;

    private int pid;

    /** Contains the node-local ID of the DataSource that produced this object. */
    private int internalId;

    public OperationStatistics() {
    }

    public OperationStatistics(String hostname, int pid, String className,
            String name, long startTime, long endTime) {
        this.hostname = hostname;
        this.pid = pid;
        this.className = className;
        this.name = name;
        this.startTime = startTime;
        this.endTime = endTime;
        internalId = -1;
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
        return endTime - startTime;
    }

    public void setDuration(long duration) {
        throw new UnsupportedOperationException("setDuration");
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
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

    public int getInternalId() {
        return internalId;
    }

    public void setInternalId(int internalId) {
        this.internalId = internalId;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append("{");
        sb.append("pid:").append(getPid());
        sb.append(",hostname:").append(getHostname());
        sb.append(",className:").append(getClassName());
        sb.append(",name:").append(getName());
        sb.append(",startTime:").append(getStartTime());
        sb.append(",endTime:").append(getEndTime());
        sb.append(",duration:").append(getDuration());
        sb.append("}");
        return sb.toString();
    }

    public Aggregator getAggregator() {
        return new Aggregator(this);
    }
}
