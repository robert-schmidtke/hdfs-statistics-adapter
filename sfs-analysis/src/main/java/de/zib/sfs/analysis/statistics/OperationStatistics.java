/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.statistics;

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

        private String hostname;

        private int pid;

        private String key;

        private long count;

        private long timeBin, timeBinDuration, cpuTime;

        private OperationSource source;

        private OperationCategory category;

        public Aggregator() {
        }

        public Aggregator(OperationStatistics statistics, long timeBinDuration) {
            hostname = statistics.getHostname();
            pid = statistics.getPid();
            key = statistics.getKey();
            count = 1;
            this.timeBinDuration = timeBinDuration;
            timeBin = statistics.getStartTime() - statistics.getStartTime()
                    % timeBinDuration;
            cpuTime = statistics.getDuration();
            source = statistics.getSource();
            category = statistics.getCategory();
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

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
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

        public long getTimeBinDuration() {
            return timeBinDuration;
        }

        public void setTimeBinDuration(long timeBinDuration) {
            this.timeBinDuration = timeBinDuration;
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

        public Aggregator aggregate(Aggregator aggregator)
                throws NotAggregatableException {
            if (this == aggregator) {
                throw new NotAggregatableException("Cannot aggregate self");
            }

            if (!aggregator.getHostname().equals(hostname)) {
                throw new NotAggregatableException("Hostnames do not match: "
                        + hostname + ", " + aggregator.getHostname());
            }

            if (aggregator.getPid() != pid) {
                throw new NotAggregatableException("Pids do not match: " + pid
                        + ", " + aggregator.getPid());
            }

            if (!aggregator.getKey().equals(key)) {
                throw new NotAggregatableException("Keys do not match: " + key
                        + ", " + aggregator.getKey());
            }

            if (aggregator.getTimeBin() != timeBin) {
                throw new NotAggregatableException("Time bins do not match: "
                        + timeBin + ", " + aggregator.getTimeBin());
            }

            if (!aggregator.getSource().equals(source)) {
                throw new NotAggregatableException("Sources do not match: "
                        + source + ", " + aggregator.getSource());
            }

            if (!aggregator.getCategory().equals(category)) {
                throw new NotAggregatableException("Categories do not match: "
                        + category + ", " + aggregator.getCategory());
            }

            count += aggregator.getCount();
            cpuTime += aggregator.getCpuTime();

            return this;
        }

        public String getCsvHeaders(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append("hostname");
            sb.append(separator).append("pid");
            sb.append(separator).append("key");
            sb.append(separator).append("source");
            sb.append(separator).append("category");
            sb.append(separator).append("count");
            sb.append(separator).append("timeBin");
            sb.append(separator).append("cpuTime");
            return sb.toString();
        }

        public String toCsv(String separator) {
            StringBuilder sb = new StringBuilder();
            sb.append(hostname);
            sb.append(separator).append(pid);
            sb.append(separator).append(key);
            sb.append(separator).append(source.name().toLowerCase());
            sb.append(separator).append(category.name().toLowerCase());
            sb.append(separator).append(count);
            sb.append(separator).append(timeBin);
            sb.append(separator).append(cpuTime);
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

    private String hostname, key, className, name, instance;

    private int pid;

    /** Contains the node-local ID of the DataSource that produced this object. */
    private int internalId;

    private OperationSource source;

    private OperationCategory category;

    public OperationStatistics() {
    }

    public OperationStatistics(String hostname, int pid, String key,
            String className, String name, String instance, long startTime,
            long endTime) {
        this.hostname = hostname;
        this.pid = pid;
        this.key = key;
        this.className = className;
        this.name = name;
        this.instance = instance;
        this.startTime = startTime;
        this.endTime = endTime;
        internalId = -1;

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

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
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

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public int getInternalId() {
        return internalId;
    }

    public void setInternalId(int internalId) {
        this.internalId = internalId;
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

    public String toCsv(String separator) {
        StringBuilder sb = new StringBuilder();
        sb.append("pid:").append(getPid());
        sb.append(separator).append("hostname:").append(getHostname());
        sb.append(separator).append("key:").append(getKey());
        sb.append(separator).append("className:").append(getClassName());
        sb.append(separator).append("name:").append(getName());
        sb.append(separator).append("instance:").append(getInstance());
        sb.append(separator).append("startTime:").append(getStartTime());
        sb.append(separator).append("endTime:").append(getEndTime());
        sb.append(separator).append("duration:").append(getDuration());
        sb.append(separator).append("source:").append(getSource());
        sb.append(separator).append("category:").append(getCategory());
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName()).append("{");
        sb.append(toCsv(","));
        sb.append("}");
        return sb.toString();
    }

    public Aggregator getAggregator(long timeBinDuration) {
        return new Aggregator(this, timeBinDuration);
    }
}
