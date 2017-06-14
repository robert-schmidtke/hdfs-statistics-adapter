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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LiveOperationStatisticsAggregator {

    private boolean initialized;

    private String systemHostname, systemKey;
    private int systemPid;

    // needs to have a positive non-zero value before it has been properly
    // initialized, as it might be used before that
    private long timeBinDuration = 1L;
    private int timeBinCacheSize;

    private String outputSeparator;
    private String logFilePrefix;

    // for each source/category combination, map a time bin to an aggregate
    // operation statistics
    private final List<NavigableMap<Long, OperationStatistics>> aggregates;

    // FIXME Not ideal data structure, there are supposedly faster concurrent
    // queues out there. Plus it may grow quite large in face of high
    // concurrency.
    private final Queue<OperationStatistics> overflowQueue;

    private final BufferedWriter[] writers;
    private final Object[] writerLocks;

    private final ForkJoinPool threadPool;

    // we roll our own file descriptors because the ones issued by the OS can be
    // reused, but won't be if the file is not closed, so we just try and give a
    // small integer to each file
    private final AtomicInteger currentFileDescriptor;

    // mapping of file names to their first file descriptors
    private final Map<String, Integer> fileDescriptors;

    public static final LiveOperationStatisticsAggregator instance = new LiveOperationStatisticsAggregator();

    private LiveOperationStatisticsAggregator() {
        // map each source/category combination, map a time bin to an aggregate
        aggregates = new ArrayList<NavigableMap<Long, OperationStatistics>>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregates.add(new ConcurrentSkipListMap<>());
        }

        overflowQueue = new ConcurrentLinkedQueue<>();

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

        fileDescriptors = new ConcurrentHashMap<>();
        currentFileDescriptor = new AtomicInteger(0);

        initialized = false;
    }

    public void initialize() {
        synchronized (this) {
            if (initialized) {
                return;
            }
            initialized = true;
        }

        outputSeparator = ",";

        // guard against weird hostnames
        systemHostname = System.getProperty("de.zib.sfs.hostname")
                .replaceAll(outputSeparator, "");

        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long
                .parseLong(System.getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer
                .parseInt(System.getProperty("de.zib.sfs.timeBin.cacheSize"));
        String outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");

        logFilePrefix = outputDirectory
                + (outputDirectory.endsWith(File.separator) ? ""
                        : File.separator)
                + systemHostname + "." + systemPid + "." + systemKey;
    }

    public int getFileDescriptor(String filename) {
        if (!initialized || filename == null) {
            return -1;
        }

        // reuses file descriptors for the same file
        return fileDescriptors.computeIfAbsent(filename,
                s -> currentFileDescriptor.incrementAndGet());
    }

    public void aggregateOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime) {
        if (!initialized) {
            return;
        }

        try {
            threadPool.execute(
                    new AggregationTask(source, category, startTime, endTime));
        } catch (RejectedExecutionException e) {
            overflowQueue.add(new OperationStatistics(timeBinDuration, source,
                    category, startTime, endTime));
        }
    }

    public void aggregateDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime,
            long data) {
        if (!initialized) {
            return;
        }

        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, data));
        } catch (RejectedExecutionException e) {
            overflowQueue.add(new DataOperationStatistics(timeBinDuration,
                    source, category, startTime, endTime, data));
        }
    }

    public void aggregateReadDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, long data,
            boolean isRemote) {
        if (!initialized) {
            return;
        }

        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, data, isRemote));
        } catch (RejectedExecutionException e) {
            overflowQueue.add(new ReadDataOperationStatistics(timeBinDuration,
                    source, category, startTime, endTime, data, isRemote));
        }
    }

    public synchronized void flush() {
        aggregates.forEach(v -> {
            Map.Entry<Long, OperationStatistics> entry = v.pollFirstEntry();
            while (entry != null) {
                try {
                    write(entry.getValue());
                    entry = v.pollFirstEntry();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void shutdown() {
        if (!initialized) {
            return;
        }

        // wait a bit for all currently running threads before submitting the
        // processing of the overflow queue
        if (!threadPool.awaitQuiescence(30, TimeUnit.SECONDS)) {
            System.err.println("Thread pool did not quiesce");
        }

        if (!overflowQueue.isEmpty()) {
            for (int i = 0; i < Runtime.getRuntime()
                    .availableProcessors(); ++i) {
                threadPool.execute(new AggregationTask(overflowQueue.poll()));
            }

            if (!threadPool.awaitQuiescence(30, TimeUnit.SECONDS)) {
                System.err.println("Thread pool did not quiesce");
            }
        }

        // stop accepting new tasks
        synchronized (this) {
            if (!initialized) {
                return;
            }
            initialized = false;
        }
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

        // write out the descriptor mappings
        try {
            BufferedWriter fileDescriptorMappingsWriter = new BufferedWriter(
                    new FileWriter(new File(getLogFilePrefix()
                            + ".filedescriptormappings.csv")));
            fileDescriptorMappingsWriter.write("filedescriptor,filename");
            fileDescriptorMappingsWriter.newLine();
            for (Map.Entry<String, Integer> fd : fileDescriptors.entrySet()) {
                fileDescriptorMappingsWriter
                        .write(fd.getValue() + "," + fd.getKey());
                fileDescriptorMappingsWriter.newLine();
            }
            fileDescriptorMappingsWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getLogFilePrefix() {
        return logFilePrefix;
    }

    public String getOutputSeparator() {
        return outputSeparator;
    }

    public boolean isInitialized() {
        return initialized;
    }

    private void write(OperationStatistics aggregate) throws IOException {
        int index = getUniqueIndex(aggregate.getSource(),
                aggregate.getCategory());
        synchronized (writerLocks[index]) {
            if (writers[index] == null) {
                String filename = getLogFilePrefix() + "."
                        + aggregate.getSource().name().toLowerCase() + "."
                        + aggregate.getCategory().name().toLowerCase() + "."
                        + System.currentTimeMillis() + ".csv";

                File file = new File(filename);
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
            case ZIP:
                return 6;
            }
        case SFS:
            switch (category) {
            case READ:
                return 3;
            case WRITE:
                return 4;
            case OTHER:
                return 5;
            case ZIP:
                return 7;
            }
        }
        throw new IllegalArgumentException(
                source.name() + "/" + category.name());
    }

    private class AggregationTask extends ForkJoinTask<Void> {

        private static final long serialVersionUID = -6851294902690575903L;

        private OperationStatistics aggregate;

        public AggregationTask(OperationStatistics aggregate) {
            this.aggregate = aggregate;
        }

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
            while (aggregate != null) {
                // get the time bin applicable for this operation
                NavigableMap<Long, OperationStatistics> timeBins = aggregates
                        .get(getUniqueIndex(aggregate.getSource(),
                                aggregate.getCategory()));
                timeBins.merge(aggregate.getTimeBin(), aggregate, (v1, v2) -> {
                    try {
                        return v1.aggregate(v2);
                    } catch (OperationStatistics.NotAggregatableException e) {
                        throw new IllegalArgumentException(e);
                    }
                });

                // make sure to emit aggregates when the cache is full until
                // it's half full again to avoid writing every time bin size
                // from now on
                int size = timeBins.size();
                if (size > timeBinCacheSize) {
                    for (int i = size / 2; i > 0; --i) {
                        try {
                            Map.Entry<Long, OperationStatistics> entry = timeBins
                                    .pollFirstEntry();
                            if (entry != null) {
                                write(entry.getValue());
                            } else {
                                break;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                aggregate = overflowQueue.poll();
            }

            return true;
        }
    }
}
