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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.FileChannel;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LiveOperationStatisticsAggregator {

    public static enum OutputFormat {
        CSV, FB, BB;
    }

    private boolean initialized;

    private String systemHostname, systemKey;
    private int systemPid;

    // needs to have a positive non-zero value before it has been properly
    // initialized, as it might be used before that
    private long timeBinDuration = 1L;
    private int timeBinCacheSize;

    private OutputFormat outputFormat;

    private String csvOutputSeparator;
    private String logFilePrefix;

    // only have one thread emit from the aggregates
    private static final AtomicBoolean EMISSION_IN_PROGRESS = new AtomicBoolean(
            false);

    // for each source/category combination, map a time bin to an aggregate
    // operation statistics for each file
    private final List<NavigableMap<Long, NavigableMap<Integer, OperationStatistics>>> aggregates;

    // FIXME Not ideal data structure, there are supposedly faster concurrent
    // queues out there. Plus it may grow quite large in face of high
    // concurrency.
    private final Queue<OperationStatistics> overflowQueue;

    // for coordinating writing of statistics
    private final Object[] writerLocks;

    // for CSV output
    private StringBuilder[] csvStringBuilders;
    private BufferedWriter[] csvWriters;

    // for FlatBuffer/ByteBuffer output
    private FileChannel[] bbChannels;

    private final ForkJoinPool threadPool;

    // we roll our own file descriptors because the ones issued by the OS can be
    // reused, but won't be if the file is not closed, so we just try and give a
    // small integer to each file
    private final AtomicInteger currentFileDescriptor;

    // mapping of file names to their first file descriptors
    private final Map<String, Integer> fileDescriptors;
    private boolean traceFileDescriptors;

    private long initializationTime;

    public static final LiveOperationStatisticsAggregator instance = new LiveOperationStatisticsAggregator();

    private LiveOperationStatisticsAggregator() {
        // map each source/category combination, map a time bin to an aggregate
        aggregates = new ArrayList<NavigableMap<Long, NavigableMap<Integer, OperationStatistics>>>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregates.add(new ConcurrentSkipListMap<>());
        }

        overflowQueue = new ConcurrentLinkedQueue<>();

        // similar for the writer locks
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

        outputFormat = OutputFormat.BB;
        switch (outputFormat) {
        case CSV:
            csvStringBuilders = new StringBuilder[OperationSource
                    .values().length * OperationCategory.values().length];
            csvWriters = new BufferedWriter[OperationSource.values().length
                    * OperationCategory.values().length];
            break;
        case FB:
        case BB:
            bbChannels = new FileChannel[OperationSource.values().length
                    * OperationCategory.values().length];
            break;
        default:
            throw new IllegalArgumentException(outputFormat.name());
        }

        csvOutputSeparator = ",";

        // guard against weird hostnames
        systemHostname = System.getProperty("de.zib.sfs.hostname")
                .replaceAll(csvOutputSeparator, "");

        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long
                .parseLong(System.getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer
                .parseInt(System.getProperty("de.zib.sfs.timeBin.cacheSize"));
        String outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");

        traceFileDescriptors = Boolean
                .parseBoolean(System.getProperty("de.zib.sfs.traceFds"));

        logFilePrefix = outputDirectory
                + (outputDirectory.endsWith(File.separator) ? ""
                        : File.separator)
                + systemHostname + "." + systemPid + "." + systemKey;

        initializationTime = System.currentTimeMillis();
    }

    public int getFileDescriptor(String filename) {
        if (!initialized || filename == null || !traceFileDescriptors) {
            return 0;
        }

        // reuses file descriptors for the same file
        return fileDescriptors.computeIfAbsent(filename,
                s -> currentFileDescriptor.incrementAndGet());
    }

    public void aggregateOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd) {
        if (!initialized) {
            return;
        }

        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, fd));
        } catch (RejectedExecutionException e) {
            overflowQueue.add(new OperationStatistics(timeBinDuration, source,
                    category, startTime, endTime, fd));
        }
    }

    public void aggregateDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data) {
        if (!initialized) {
            return;
        }

        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, fd, data));
        } catch (RejectedExecutionException e) {
            overflowQueue.add(new DataOperationStatistics(timeBinDuration,
                    source, category, startTime, endTime, fd, data));
        }
    }

    public void aggregateReadDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data, boolean isRemote) {
        if (!initialized) {
            return;
        }

        try {
            threadPool.execute(new AggregationTask(source, category, startTime,
                    endTime, fd, data, isRemote));
        } catch (RejectedExecutionException e) {
            overflowQueue.add(new ReadDataOperationStatistics(timeBinDuration,
                    source, category, startTime, endTime, fd, data, isRemote));
        }
    }

    public synchronized void flush() {
        aggregates.forEach(v -> {
            Map.Entry<Long, NavigableMap<Integer, OperationStatistics>> entry = v
                    .pollFirstEntry();
            while (entry != null) {
                try {
                    for (OperationStatistics os : entry.getValue().values())
                        write(os);

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
        switch (outputFormat) {
        case CSV:
            for (BufferedWriter writer : csvWriters)
                if (writer != null)
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            break;
        case FB:
        case BB:
            for (FileChannel channel : bbChannels)
                if (channel != null)
                    try {
                        channel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            break;
        default:
            throw new IllegalArgumentException(outputFormat.name());
        }

        // write out the descriptor mappings
        if (traceFileDescriptors) {
            try {
                BufferedWriter fileDescriptorMappingsWriter = new BufferedWriter(
                        new FileWriter(new File(
                                getLogFilePrefix() + ".filedescriptormappings."
                                        + initializationTime + ".csv")));

                fileDescriptorMappingsWriter.write("hostname");
                fileDescriptorMappingsWriter.write(csvOutputSeparator);
                fileDescriptorMappingsWriter.write("pid");
                fileDescriptorMappingsWriter.write(csvOutputSeparator);
                fileDescriptorMappingsWriter.write("key");
                fileDescriptorMappingsWriter.write(csvOutputSeparator);
                fileDescriptorMappingsWriter.write("fileDescriptor");
                fileDescriptorMappingsWriter.write(csvOutputSeparator);
                fileDescriptorMappingsWriter.write("filename");
                fileDescriptorMappingsWriter.newLine();

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> fd : fileDescriptors
                        .entrySet()) {
                    sb.append(systemHostname).append(csvOutputSeparator);
                    sb.append(systemPid).append(csvOutputSeparator);
                    sb.append(systemKey).append(csvOutputSeparator);
                    sb.append(fd.getValue()).append(csvOutputSeparator);
                    sb.append(fd.getKey());
                    fileDescriptorMappingsWriter.write(sb.toString());
                    fileDescriptorMappingsWriter.newLine();
                    sb.setLength(0);
                }

                fileDescriptorMappingsWriter.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getLogFilePrefix() {
        return logFilePrefix;
    }

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    public String getOutputSeparator() {
        return csvOutputSeparator;
    }

    public boolean isInitialized() {
        return initialized;
    }

    private void write(OperationStatistics aggregate) throws IOException {
        switch (outputFormat) {
        case CSV:
            writeCsv(aggregate);
            break;
        case FB:
        case BB:
            writeBinary(aggregate);
            break;
        default:
            throw new IllegalArgumentException(outputFormat.name());
        }
    }

    private void writeCsv(OperationStatistics aggregate) throws IOException {
        int index = getUniqueIndex(aggregate.getSource(),
                aggregate.getCategory());
        synchronized (writerLocks[index]) {
            if (csvStringBuilders[index] == null) {
                csvStringBuilders[index] = new StringBuilder();
            } else {
                csvStringBuilders[index].setLength(0);
            }

            if (csvWriters[index] == null) {
                String filename = getLogFilePrefix() + "."
                        + aggregate.getSource().name().toLowerCase() + "."
                        + aggregate.getCategory().name().toLowerCase() + "."
                        + initializationTime + "."
                        + outputFormat.name().toLowerCase();

                File file = new File(filename);
                if (!file.exists()) {
                    csvStringBuilders[index].append("hostname");
                    csvStringBuilders[index].append(csvOutputSeparator)
                            .append("pid");
                    csvStringBuilders[index].append(csvOutputSeparator)
                            .append("key");
                    csvStringBuilders[index].append(csvOutputSeparator).append(
                            aggregate.getCsvHeaders(csvOutputSeparator));

                    // we will receive writes to this file as well
                    csvWriters[index] = new BufferedWriter(
                            new FileWriter(file));
                    csvWriters[index]
                            .write(csvStringBuilders[index].toString());
                    csvWriters[index].newLine();

                    csvStringBuilders[index].setLength(0);
                } else {
                    throw new IOException(filename + " already exists");
                }
            }

            csvStringBuilders[index].append(systemHostname);
            csvStringBuilders[index].append(csvOutputSeparator)
                    .append(systemPid);
            csvStringBuilders[index].append(csvOutputSeparator)
                    .append(systemKey);
            csvStringBuilders[index].append(csvOutputSeparator)
                    .append(aggregate.toCsv(csvOutputSeparator));

            csvWriters[index].write(csvStringBuilders[index].toString());
            csvWriters[index].newLine();
        }
    }

    @SuppressWarnings("resource") // we close the channels on shutdown
    private void writeBinary(OperationStatistics aggregate) throws IOException {
        int index = getUniqueIndex(aggregate.getSource(),
                aggregate.getCategory());
        synchronized (writerLocks[index]) {
            if (bbChannels[index] == null) {
                String filename = getLogFilePrefix() + "."
                        + aggregate.getSource().name().toLowerCase() + "."
                        + aggregate.getCategory().name().toLowerCase() + "."
                        + initializationTime + "."
                        + outputFormat.name().toLowerCase();

                File file = new File(filename);
                if (!file.exists()) {
                    bbChannels[index] = new FileOutputStream(file).getChannel();
                } else {
                    throw new IOException(filename + " already exists");
                }
            }

            switch (outputFormat) {
            case FB:
                bbChannels[index].write(aggregate.toFlatBuffer(systemHostname,
                        systemPid, systemKey));
                break;
            case BB:
                bbChannels[index].write(aggregate.toByteBuffer(systemHostname,
                        systemPid, systemKey));
                break;
            default:
                throw new IllegalArgumentException(outputFormat.name());
            }
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
                int fd, long data, boolean isRemote) {
            aggregate = new ReadDataOperationStatistics(timeBinDuration, source,
                    category, startTime, endTime, fd, data, isRemote);
        }

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime,
                int fd, long data) {
            aggregate = new DataOperationStatistics(timeBinDuration, source,
                    category, startTime, endTime, fd, data);
        }

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime,
                int fd) {
            aggregate = new OperationStatistics(timeBinDuration, source,
                    category, startTime, endTime, fd);
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
                NavigableMap<Long, NavigableMap<Integer, OperationStatistics>> timeBins = aggregates
                        .get(getUniqueIndex(aggregate.getSource(),
                                aggregate.getCategory()));

                // get the file descriptor applicable for this operation
                NavigableMap<Integer, OperationStatistics> fileDescriptors = timeBins
                        .computeIfAbsent(aggregate.getTimeBin(),
                                l -> new ConcurrentSkipListMap<>());

                fileDescriptors.merge(aggregate.getFileDescriptor(), aggregate,
                        (v1, v2) -> {
                            try {
                                return v1.aggregate(v2);
                            } catch (OperationStatistics.NotAggregatableException e) {
                                e.printStackTrace();
                                throw new IllegalArgumentException(e);
                            }
                        });

                // make sure to emit aggregates when the cache is full until
                // it's half full again to avoid writing every time bin size
                // from now on, only have one thread do the emission check
                if (!EMISSION_IN_PROGRESS.getAndSet(true)) {
                    // emission was not in progress, all other threads now see
                    // it as in progress and skip this
                    int size = timeBins.size();
                    if (size > timeBinCacheSize) {
                        for (int i = size / 2; i > 0; --i) {
                            try {
                                Map.Entry<Long, NavigableMap<Integer, OperationStatistics>> entry = timeBins
                                        .pollFirstEntry();
                                if (entry != null) {
                                    for (OperationStatistics os : entry
                                            .getValue().values())
                                        write(os);
                                } else {
                                    break;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        try {
                            switch (outputFormat) {
                            case CSV:
                                for (Writer w : csvWriters)
                                    if (w != null)
                                        w.flush();
                                break;
                            case FB:
                            case BB:
                                for (FileChannel fc : bbChannels)
                                    if (fc != null)
                                        fc.force(false);
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        outputFormat.name());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // reset emission in progress state
                    EMISSION_IN_PROGRESS.set(false);
                }

                aggregate = overflowQueue.poll();
            }

            return true;
        }
    }
}
