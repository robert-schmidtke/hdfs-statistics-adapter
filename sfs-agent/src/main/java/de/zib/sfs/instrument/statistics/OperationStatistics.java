/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

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

        private long timeBin, timeBinDuration, cpuTime;

        private OperationSource source;

        private OperationCategory category;

        public Aggregator() {
        }

        public Aggregator(OperationStatistics statistics, long timeBinDuration) {
            count = 1;
            this.timeBinDuration = timeBinDuration;
            timeBin = statistics.getStartTime() - statistics.getStartTime()
                    % timeBinDuration;
            cpuTime = statistics.getDuration();
            source = statistics.getSource();
            category = statistics.getCategory();
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

    private long startTime, endTime;

    private OperationSource source;

    private OperationCategory category;

    public OperationStatistics() {
    }

    public OperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime) {
        this.source = source;
        this.category = category;
        this.startTime = startTime;
        this.endTime = endTime;
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
        sb.append("startTime:").append(startTime);
        sb.append(separator).append("endTime:").append(endTime);
        sb.append(separator).append("duration:").append(endTime - startTime);
        sb.append(separator).append("source:").append(source);
        sb.append(separator).append("category:").append(category);
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
