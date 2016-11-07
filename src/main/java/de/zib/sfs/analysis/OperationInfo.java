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

    protected final String hostname, name;

    public static class Aggregator {

        protected long count;

        protected long duration, minDuration, maxDuration;

        public Aggregator() {
            count = 0L;
            duration = 0L;
            minDuration = Long.MAX_VALUE;
            maxDuration = Long.MIN_VALUE;
        }

        public void aggregate(OperationInfo operationInfo) {
            ++count;
            duration += operationInfo.getDuration();
            minDuration = Math.min(minDuration, operationInfo.getDuration());
            maxDuration = Math.max(maxDuration, operationInfo.getDuration());
        }

        public long getCount() {
            return count;
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

    }

    public OperationInfo(String hostname, String name, long startTime,
            long endTime) {
        this.hostname = hostname;
        this.name = name;
        this.startTime = startTime;
        this.endTime = endTime;
        duration = endTime - startTime;
    }

    public String getHostname() {
        return hostname;
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

    public Aggregator getAggregator() {
        return new Aggregator();
    }
}
