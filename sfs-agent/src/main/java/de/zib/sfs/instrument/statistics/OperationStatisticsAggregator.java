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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import de.zib.sfs.instrument.statistics.OperationStatistics.Aggregator.NotAggregatableException;

public class OperationStatisticsAggregator {

    private boolean initialized;

    private String systemHostname, systemKey;
    private int systemPid;

    // needs to have a positive non-zero value before it has been properly
    // initialized, as it might be used before that
    private long timeBinDuration = 1L;
    private int timeBinCacheSize;

    private String outputDirectory, outputSeparator;

    // for each source/category combination, map a time bin to an aggregator
    private final List<ConcurrentSkipListMap<Long, OperationStatistics.Aggregator>> aggregators;

    private final BufferedWriter[] writers;
    private final Object[] writerLocks;

    private final ForkJoinPool threadPool;

    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS");

    public static final OperationStatisticsAggregator instance = new OperationStatisticsAggregator();

    private OperationStatisticsAggregator() {
        // map each source/category combination, map a time bin to an aggregator
        aggregators = new ArrayList<ConcurrentSkipListMap<Long, OperationStatistics.Aggregator>>();
        for (int i = 0; i < OperationSource.values().length * OperationCategory.values().length; ++i) {
            aggregators.add(new ConcurrentSkipListMap<>());
        }

        // similar for the writers
        writers = new BufferedWriter[OperationSource.values().length * OperationCategory.values().length];
        writerLocks = new Object[OperationSource.values().length * OperationCategory.values().length];
        for (OperationSource source : OperationSource.values()) {
            for (OperationCategory category : OperationCategory.values()) {
                writerLocks[getUniqueIndex(source, category)] = new Object();
            }
        }

        // worker pool that will accept the aggregation tasks
        threadPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors(), ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

        initialized = false;
    }

    public void initialize() {
        if (initialized) {
            return;
        }

        systemHostname = System.getProperty("de.zib.sfs.hostname");

        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long.parseLong(System.getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer.parseInt(System.getProperty("de.zib.sfs.timeBin.cacheSize"));
        this.outputDirectory = System.getProperty("de.zib.sfs.output.directory");
        outputSeparator = ",";

        initialized = true;
    }

    public void aggregateOperationStatistics(OperationSource source, OperationCategory category, long startTime, long endTime) {
        try {
            threadPool.execute(new AggregationTask(source, category, startTime, endTime));
        } catch (RejectedExecutionException e) {
        }
    }

    public void aggregateDataOperationStatistics(OperationSource source, OperationCategory category, long startTime, long endTime, long data) {
        try {
            threadPool.execute(new AggregationTask(source, category, startTime, endTime, data));
        } catch (RejectedExecutionException e) {
        }
    }

    public void aggregateReadDataOperationStatistics(OperationSource source, OperationCategory category, long startTime, long endTime, long data,
            boolean isRemote) {
        try {
            threadPool.execute(new AggregationTask(source, category, startTime, endTime, data, isRemote));
        } catch (RejectedExecutionException e) {
        }
    }

    public void shutdown() {
        if (!initialized) {
            return;
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

    private void write(OperationStatistics.Aggregator aggregator) throws IOException {
        int index = getUniqueIndex(aggregator.getSource(), aggregator.getCategory());
        synchronized (writerLocks[index]) {
            if (writers[index] == null) {
                String filename = systemHostname + "." + systemPid + "." + systemKey + "." + aggregator.getSource().name().toLowerCase() + "."
                        + aggregator.getCategory().name().toLowerCase() + "." + dateFormat.format(new Date()) + ".csv";

                File file = new File(outputDirectory, filename);
                if (!file.exists()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("hostname");
                    sb.append(outputSeparator).append("pid");
                    sb.append(outputSeparator).append("key");
                    sb.append(outputSeparator).append(aggregator.getCsvHeaders(outputSeparator));

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
            sb.append(outputSeparator).append(aggregator.toCsv(outputSeparator));

            writers[index].write(sb.toString());
            writers[index].newLine();
            writers[index].flush();
        }
    }

    private int getUniqueIndex(OperationSource source, OperationCategory category) {
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

        private final OperationStatistics.Aggregator aggregator;

        public AggregationTask(OperationSource source, OperationCategory category, long startTime, long endTime, long data, boolean isRemote) {
            aggregator = new ReadDataOperationStatistics.Aggregator(timeBinDuration, source, category, startTime, endTime, data, isRemote);
        }

        public AggregationTask(OperationSource source, OperationCategory category, long startTime, long endTime, long data) {
            aggregator = new DataOperationStatistics.Aggregator(timeBinDuration, source, category, startTime, endTime, data);
        }

        public AggregationTask(OperationSource source, OperationCategory category, long startTime, long endTime) {
            aggregator = new OperationStatistics.Aggregator(timeBinDuration, source, category, startTime, endTime);
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
            aggregators.get(getUniqueIndex(aggregator.getSource(), aggregator.getCategory())).merge(aggregator.getTimeBin(), aggregator, (v1, v2) -> {
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
    }
}
