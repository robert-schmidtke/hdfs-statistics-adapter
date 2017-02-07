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
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import de.zib.sfs.instrument.statistics.OperationStatistics.Aggregator.NotAggregatableException;

public class OperationStatisticsAggregator {

    private final String systemHostname, systemKey;
    private final int systemPid;

    private final long timeBinDuration;
    private final int timeBinCacheSize;

    private final String outputDirectory, outputSeparator;

    // for each source/category combination, map a time bin to an aggregator
    private final List<ConcurrentSkipListMap<Long, OperationStatistics.Aggregator>> aggregators;

    private final BufferedWriter[] writers;
    private final Object[] writerLocks;

    private final ForkJoinPool threadPool;

    private static final Object initLock = new Object();
    private static OperationStatisticsAggregator instance;

    public static OperationStatisticsAggregator getInstance() {
        if (instance == null) {
            // acquire lock only when it might be necessary
            synchronized (initLock) {
                if (instance == null) {
                    try {
                        instance = new OperationStatisticsAggregator();
                    } catch (IllegalStateException e) {
                        // swallow, it's too early during JVM startup
                    }
                }
            }
        }
        return instance;
    }

    private OperationStatisticsAggregator() {
        systemHostname = System.getProperty("de.zib.sfs.hostname");
        if (systemHostname == null) {
            throw new IllegalStateException("Agent not yet initialized");
        }

        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long.parseLong(System
                .getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer.parseInt(System
                .getProperty("de.zib.sfs.timeBin.cacheSize"));
        this.outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");
        outputSeparator = ",";

        // map each source/category combination, map a time bin to an aggregator
        aggregators = new ArrayList<ConcurrentSkipListMap<Long, OperationStatistics.Aggregator>>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregators.add(new ConcurrentSkipListMap<>());
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
        threadPool = new ForkJoinPool(Runtime.getRuntime()
                .availableProcessors(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
    }

    public void aggregate(final OperationStatistics operationStatistics) {
        // asynchronously schedule aggregation
        final ForkJoinTask<Void> task = new ForkJoinTask<Void>() {
            private static final long serialVersionUID = -5794694736335116368L;

            @Override
            public Void getRawResult() {
                return null;
            }

            @Override
            protected void setRawResult(Void value) {
            }

            @Override
            protected boolean exec() {
                // get the time bin applicable for this operation
                OperationStatistics.Aggregator aggregator = operationStatistics
                        .getAggregator(timeBinDuration);
                aggregators.get(
                        getUniqueIndex(aggregator.getSource(),
                                aggregator.getCategory())).merge(
                        aggregator.getTimeBin(), aggregator, (v1, v2) -> {
                            try {
                                return v1.aggregate(v2);
                            } catch (NotAggregatableException e) {
                                throw new IllegalArgumentException(e);
                            }
                        });

                // make sure to emit aggregates when the cache is full
                aggregators.forEach(v -> {
                    for (int i = v.size() - timeBinCacheSize; i > 0; --i) {
                        try {
                            write(v.remove(v.firstKey()));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });

                return true;
            }
        };

        try {
            threadPool.execute(task);
        } catch (RejectedExecutionException e) {
            // when the pool is shut down already
        }
    }

    public void shutdown() {
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

        // write remaining aggregators
        aggregators.forEach(v -> {
            for (int i = v.size(); i > 0; --i) {
                try {
                    write(v.remove(v.firstKey()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

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

    private void write(OperationStatistics.Aggregator aggregator)
            throws IOException {
        int index = getUniqueIndex(aggregator.getSource(),
                aggregator.getCategory());
        synchronized (writerLocks[index]) {
            if (writers[index] == null) {
                String filename = systemHostname + "." + systemPid + "."
                        + systemKey + "."
                        + aggregator.getSource().name().toLowerCase() + "."
                        + aggregator.getCategory().name().toLowerCase()
                        + ".csv";

                File file = new File(outputDirectory, filename);
                if (!file.exists()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("hostname");
                    sb.append(outputSeparator).append("pid");
                    sb.append(outputSeparator).append("key");
                    sb.append(outputSeparator).append(
                            aggregator.getCsvHeaders(outputSeparator));

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
            sb.append(outputSeparator)
                    .append(aggregator.toCsv(outputSeparator));

            writers[index].write(sb.toString());
            writers[index].newLine();
        }
    }

    private int getUniqueIndex(OperationSource source,
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
}
