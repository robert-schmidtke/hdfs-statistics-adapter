/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class LiveOperationStatisticsAggregator {

    private boolean initialized;

    private String systemHostname, systemKey;
    private int systemPid;

    // needs to have a positive non-zero value before it has been properly
    // initialized, as it might be used before that
    private long timeBinDuration = 1L;
    private int timeBinCacheSize;

    private String outputDirectory, outputSeparator;

    // for each source/category combination, map a time bin to an aggregate
    // operation statistics
    private final List<SortedMap<Long, OperationStatistics>> aggregates;

    private final BufferedWriter[] writers;
    private final Object[] writerLocks;

    private final ForkJoinPool threadPool;

    public static final LiveOperationStatisticsAggregator instance = new LiveOperationStatisticsAggregator();

    private LiveOperationStatisticsAggregator() {
        // map each source/category combination, map a time bin to an aggregate
        aggregates = new ArrayList<SortedMap<Long, OperationStatistics>>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregates.add(new ConcurrentSkipListMap<>());
        }

        // similar for the writers
        writers = new BufferedWriter[OperationSource.values().length
                * OperationCategory.values().length];
        writerLocks = new Object[OperationSource.values().length
                * OperationCategory.values().length];
        for (OperationSource source : OperationSource.values()) {
            for (OperationCategory category : OperationCategory.values()) {
                writerLocks[getUniqueIndex(source, category)] = new Object();
            }
        }

        // worker pool that will accept the aggregation tasks
        threadPool = new ForkJoinPool(
                Runtime.getRuntime().availableProcessors(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

        initialized = false;
    }

    public void initialize() {
        synchronized (this) {
            if (initialized) {
                return;
            }
            initialized = true;
        }

        systemHostname = System.getProperty("de.zib.sfs.hostname");

        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long
                .parseLong(System.getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer
                .parseInt(System.getProperty("de.zib.sfs.timeBin.cacheSize"));
        this.outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");
        outputSeparator = ",";
    }

    public void aggregateOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime) {
        try {
            threadPool.execute(
                    new AggregationTask(source, category, startTime, endTime));
        } catch (RejectedExecutionException e) {
        }
    }

    public void aggregateDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime,
            long data) {
        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, data));
        } catch (RejectedExecutionException e) {
        }
    }

    public void aggregateReadDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, long data,
            boolean isRemote) {
        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, data, isRemote));
        } catch (RejectedExecutionException e) {
        }
    }

    public synchronized void flush() {
        aggregates.forEach(v -> {
            for (int i = v.size(); i > 0; --i) {
                try {
                    write(v.remove(v.firstKey()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void shutdown() {
        synchronized (this) {
            if (!initialized) {
                return;
            }
            initialized = false;
        }

        // wait a bit for all currently running threads before shutting down
        if (!threadPool.awaitQuiescence(30, TimeUnit.SECONDS)) {
            System.err.println("Thread pool did not quiesce");
        }

        // stop accepting new tasks
        threadPool.shutdown();

        // wait a bit for all still currently running tasks
        try {
            if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Thread pool did not shut down");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // write remaining aggregates
        flush();

        // finally close all writers
        for (BufferedWriter writer : writers) {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void write(OperationStatistics aggregate) throws IOException {
        int index = getUniqueIndex(aggregate.getSource(),
                aggregate.getCategory());
        synchronized (writerLocks[index]) {
            if (writers[index] == null) {
                String filename = systemHostname + "." + systemPid + "."
                        + systemKey + "."
                        + aggregate.getSource().name().toLowerCase() + "."
                        + aggregate.getCategory().name().toLowerCase() + "."
                        + System.currentTimeMillis() + ".csv";

                File file = new File(outputDirectory, filename);
                if (!file.exists()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("hostname");
                    sb.append(outputSeparator).append("pid");
                    sb.append(outputSeparator).append("key");
                    sb.append(outputSeparator)
                            .append(aggregate.getCsvHeaders(outputSeparator));

                    // we will receive writes to this file as well
                    writers[index] = new BufferedWriter(new FileWriter(file));
                    writers[index].write(sb.toString());
                    writers[index].newLine();
                } else {
                    throw new IOException(filename + " already exists");
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append(systemHostname);
            sb.append(outputSeparator).append(systemPid);
            sb.append(outputSeparator).append(systemKey);
            sb.append(outputSeparator).append(aggregate.toCsv(outputSeparator));

            writers[index].write(sb.toString());
            writers[index].newLine();
            writers[index].flush();
        }
    }

    public static int getUniqueIndex(OperationSource source,
            OperationCategory category) {
        switch (source) {
        case JVM:
            switch (category) {
            case READ:
                return 0;
            case WRITE:
                return 1;
            case OTHER:
                return 2;
            default:
                throw new IllegalArgumentException(category.name());
            }
        case SFS:
            switch (category) {
            case READ:
                return 3;
            case WRITE:
                return 4;
            case OTHER:
                return 5;
            default:
                throw new IllegalArgumentException(category.name());
            }
        default:
            throw new IllegalArgumentException(source.name());
        }
    }

    private class AggregationTask extends ForkJoinTask<Void> {

        private static final long serialVersionUID = -6851294902690575903L;

        private final OperationStatistics aggregate;

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime,
                long data, boolean isRemote) {
            aggregate = new ReadDataOperationStatistics(timeBinDuration, source,
                    category, startTime, endTime, data, isRemote);
        }

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime,
                long data) {
            aggregate = new DataOperationStatistics(timeBinDuration, source,
                    category, startTime, endTime, data);
        }

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime) {
            aggregate = new OperationStatistics(timeBinDuration, source,
                    category, startTime, endTime);
        }

        @Override
        public Void getRawResult() {
            return null;
        }

        @Override
        protected void setRawResult(Void value) {
        }

        @Override
        protected boolean exec() {
            if (!initialized) {
                // pretend everything is fine
                return true;
            }

            // get the time bin applicable for this operation
            SortedMap<Long, OperationStatistics> timeBins = aggregates
                    .get(getUniqueIndex(aggregate.getSource(),
                            aggregate.getCategory()));
            timeBins.merge(aggregate.getTimeBin(), aggregate, (v1, v2) -> {
                try {
                    return v1.aggregate(v2);
                } catch (OperationStatistics.NotAggregatableException e) {
                    throw new IllegalArgumentException(e);
                }
            });

            // make sure to emit aggregates when the cache is full until it's
            // half full again to avoid writing every time bin size from now on
            synchronized (timeBins) {
                int size = timeBins.size();
                if (size > timeBinCacheSize) {
                    for (int i = size / 2; i > 0; --i) {
                        try {
                            write(timeBins.remove(timeBins.firstKey()));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }

            return true;
        }
    }

    // for testing only

    public void reset() {
        boolean assertionsEnabled = false;
        assert (assertionsEnabled = true);
        if (!assertionsEnabled) {
            throw new UnsupportedOperationException(
                    "reset() only supported with assertions enabled");
        }
        aggregates.clear();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregates.add(new ConcurrentSkipListMap<>());
        }
    }

    public List<SortedMap<Long, OperationStatistics>> getAggregates() {
        boolean assertionsEnabled = false;
        assert (assertionsEnabled = true);
        if (!assertionsEnabled) {
            throw new UnsupportedOperationException(
                    "getAggregates() only supported with assertions enabled");
        }
        return Collections.unmodifiableList(aggregates);
    }
}
