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
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
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

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.bb.FileDescriptorMappingBufferBuilder;
import de.zib.sfs.instrument.statistics.fb.FileDescriptorMappingFB;

public class LiveOperationStatisticsAggregator {

    public static enum OutputFormat {
        CSV, FB, BB;
    }

    private boolean initialized;

    private String systemHostname, systemKey;
    private ByteBuffer systemHostnameBb, systemKeyBb;
    private int systemPid;

    // needs to have a positive non-zero value before it has been properly
    // initialized, as it might be used before that
    long timeBinDuration = 1L;
    int timeBinCacheSize;

    OutputFormat outputFormat;

    private String csvOutputSeparator;
    private String logFilePrefix;

    // only have one thread emit from the aggregates
    static final AtomicBoolean EMISSION_IN_PROGRESS = new AtomicBoolean(false);

    // for each source/category combination, map a time bin to an aggregate
    // operation statistics for each file
    final List<NavigableMap<Long, NavigableMap<Integer, OperationStatistics>>> aggregates;

    // FIXME Not ideal data structure, there are supposedly faster concurrent
    // queues out there. Plus it may grow quite large in face of high
    // concurrency.
    final Queue<OperationStatistics> overflowQueue;

    // for coordinating writing of statistics
    private final Object[] writerLocks;

    // for CSV output
    private StringBuilder[] csvStringBuilders;
    BufferedWriter[] csvWriters;

    // for FlatBuffer/ByteBuffer output
    private ByteBuffer[] bbBuffers;
    FileChannel[] bbChannels;

    private final ForkJoinPool threadPool;

    // we roll our own file descriptors because the ones issued by the OS can be
    // reused, but won't be if the file is not closed, so we just try and give a
    // small integer to each file
    private final AtomicInteger currentFileDescriptor;

    // mapping of file names to their first file descriptors
    private final Map<String, Integer> filenameToFd;
    private final Map<FileDescriptor, Integer> fdToFd;
    private boolean traceFileDescriptors;

    private long initializationTime;

    public static final LiveOperationStatisticsAggregator instance = new LiveOperationStatisticsAggregator();

    private LiveOperationStatisticsAggregator() {
        // map each source/category combination, map a time bin to an aggregate
        this.aggregates = new ArrayList<>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            this.aggregates.add(new ConcurrentSkipListMap<>());
        }

        this.overflowQueue = new ConcurrentLinkedQueue<>();

        // similar for the writer locks
        this.writerLocks = new Object[OperationSource.values().length
                * OperationCategory.values().length];
        for (OperationSource source : OperationSource.values()) {
            for (OperationCategory category : OperationCategory.values()) {
                this.writerLocks[getUniqueIndex(source,
                        category)] = new Object();
            }
        }

        // worker pool that will accept the aggregation tasks
        this.threadPool = new ForkJoinPool(
                Runtime.getRuntime().availableProcessors(),
                ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);

        this.filenameToFd = new ConcurrentHashMap<>();
        this.fdToFd = new ConcurrentHashMap<>();
        this.currentFileDescriptor = new AtomicInteger(0);

        this.initialized = false;
    }

    public void initialize() {
        synchronized (this) {
            if (this.initialized) {
                return;
            }
            this.initialized = true;
        }

        this.csvOutputSeparator = ",";

        // guard against weird hostnames
        this.systemHostname = System.getProperty("de.zib.sfs.hostname")
                .replaceAll(this.csvOutputSeparator, "");

        this.systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        this.systemKey = System.getProperty("de.zib.sfs.key");

        CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();

        // pre-encode hostname
        if (this.systemHostname.length() - Byte.MAX_VALUE > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(this.systemHostname);
        }
        this.systemHostnameBb = ByteBuffer
                .allocate(this.systemHostname.length());
        encoder.reset();
        CoderResult cr = encoder.encode(CharBuffer.wrap(this.systemHostname),
                this.systemHostnameBb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(this.systemHostname, e);
            }
        }
        this.systemHostnameBb.flip();

        // same with key
        if (this.systemKey.length() - Byte.MAX_VALUE > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(this.systemKey);
        }
        this.systemKeyBb = ByteBuffer.allocate(this.systemKey.length());
        encoder.reset();
        cr = encoder.encode(CharBuffer.wrap(this.systemKey), this.systemKeyBb,
                true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(this.systemKey, e);
            }
        }
        this.systemKeyBb.flip();

        this.timeBinDuration = Long
                .parseLong(System.getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer
                .parseInt(System.getProperty("de.zib.sfs.timeBin.cacheSize"));
        String outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");
        this.outputFormat = OutputFormat.valueOf(
                System.getProperty("de.zib.sfs.output.format").toUpperCase());
        switch (this.outputFormat) {
        case CSV:
            this.csvStringBuilders = new StringBuilder[OperationSource
                    .values().length * OperationCategory.values().length];
            this.csvWriters = new BufferedWriter[OperationSource.values().length
                    * OperationCategory.values().length];
            break;
        case FB:
        case BB:
            this.bbBuffers = new ByteBuffer[OperationSource.values().length
                    * OperationCategory.values().length];
            this.bbChannels = new FileChannel[OperationSource.values().length
                    * OperationCategory.values().length];
            break;
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
        }

        this.traceFileDescriptors = Boolean
                .parseBoolean(System.getProperty("de.zib.sfs.traceFds"));

        this.logFilePrefix = outputDirectory
                + (outputDirectory.endsWith(File.separator) ? ""
                        : File.separator)
                + this.systemHostname + "." + this.systemPid + "."
                + this.systemKey;

        this.initializationTime = System.currentTimeMillis();
    }

    public String getHostname() {
        return this.systemHostname;
    }

    public int getPid() {
        return this.systemPid;
    }

    public String getKey() {
        return this.systemKey;
    }

    public int registerFileDescriptor(String filename,
            FileDescriptor fileDescriptor) {
        if (!this.initialized || filename == null
                || !this.traceFileDescriptors) {
            return 0;
        }

        // reuses file descriptors for the same file
        int fd = this.filenameToFd.computeIfAbsent(filename,
                s -> this.currentFileDescriptor.incrementAndGet());

        // there may be different file descriptor objects associated with each
        // file, so add the descriptor, even if it maps to the same fd
        if (fileDescriptor != null) {
            this.fdToFd.putIfAbsent(fileDescriptor, fd);
        }
        return fd;
    }

    public int registerFileDescriptor(String filename) {
        return registerFileDescriptor(filename, null);
    }

    public int getFileDescriptor(FileDescriptor fileDescriptor) {
        if (!this.initialized || fileDescriptor == null
                || !this.traceFileDescriptors) {
            return 0;
        }

        return this.fdToFd.getOrDefault(fileDescriptor, 0);
    }

    public void aggregateOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd) {
        if (!this.initialized) {
            return;
        }

        try {
            this.threadPool.execute(new AggregationTask(source, category,
                    startTime, endTime, fd));
        } catch (RejectedExecutionException e) {
            this.overflowQueue.add(new OperationStatistics(this.timeBinDuration,
                    source, category, startTime, endTime, fd));
        }
    }

    public void aggregateDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data) {
        if (!this.initialized) {
            return;
        }

        try {
            this.threadPool.execute(new AggregationTask(source, category,
                    startTime, endTime, fd, data));
        } catch (RejectedExecutionException e) {
            this.overflowQueue
                    .add(new DataOperationStatistics(this.timeBinDuration,
                            source, category, startTime, endTime, fd, data));
        }
    }

    public void aggregateReadDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data, boolean isRemote) {
        if (!this.initialized) {
            return;
        }

        try {
            this.threadPool.execute(new AggregationTask(source, category,
                    startTime, endTime, fd, data, isRemote));
        } catch (RejectedExecutionException e) {
            this.overflowQueue.add(new ReadDataOperationStatistics(
                    this.timeBinDuration, source, category, startTime, endTime,
                    fd, data, isRemote));
        }
    }

    public synchronized void flush() {
        this.aggregates.forEach(v -> {
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
        if (!this.initialized) {
            return;
        }

        // wait a bit for all currently running threads before submitting the
        // processing of the overflow queue
        if (!this.threadPool.awaitQuiescence(30, TimeUnit.SECONDS)) {
            System.err.println("Thread pool did not quiesce");
        }

        if (!this.overflowQueue.isEmpty()) {
            for (int i = 0; i < Runtime.getRuntime()
                    .availableProcessors(); ++i) {
                this.threadPool.execute(
                        new AggregationTask(this.overflowQueue.poll()));
            }

            if (!this.threadPool.awaitQuiescence(30, TimeUnit.SECONDS)) {
                System.err.println("Thread pool did not quiesce");
            }
        }

        // stop accepting new tasks
        synchronized (this) {
            if (!this.initialized) {
                return;
            }
            this.initialized = false;
        }
        this.threadPool.shutdown();

        // wait a bit for all still currently running tasks
        try {
            if (!this.threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Thread pool did not shut down");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // write remaining aggregates
        flush();

        // finally close all writers
        switch (this.outputFormat) {
        case CSV:
            for (BufferedWriter writer : this.csvWriters)
                if (writer != null)
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            break;
        case FB:
        case BB:
            for (FileChannel channel : this.bbChannels)
                if (channel != null)
                    try {
                        channel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            break;
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
        }

        // write out the descriptor mappings
        if (this.traceFileDescriptors) {
            writeFileDescriptorMappings();
        }
    }

    public String getLogFilePrefix() {
        return this.logFilePrefix;
    }

    public OutputFormat getOutputFormat() {
        return this.outputFormat;
    }

    public String getCsvOutputSeparator() {
        return this.csvOutputSeparator;
    }

    public boolean isInitialized() {
        return this.initialized;
    }

    void write(OperationStatistics aggregate) throws IOException {
        switch (this.outputFormat) {
        case CSV:
            writeCsv(aggregate);
            break;
        case FB:
        case BB:
            writeBinary(aggregate);
            break;
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
        }
    }

    private void writeCsv(OperationStatistics aggregate) throws IOException {
        int index = getUniqueIndex(aggregate.getSource(),
                aggregate.getCategory());
        synchronized (this.writerLocks[index]) {
            if (this.csvStringBuilders[index] == null) {
                this.csvStringBuilders[index] = new StringBuilder(256);
            } else {
                this.csvStringBuilders[index].setLength(0);
            }

            if (this.csvWriters[index] == null) {
                String filename = getLogFilePrefix() + "."
                        + aggregate.getSource().name().toLowerCase() + "."
                        + aggregate.getCategory().name().toLowerCase() + "."
                        + this.initializationTime + "."
                        + this.outputFormat.name().toLowerCase();

                File file = new File(filename);
                if (!file.exists()) {
                    this.csvStringBuilders[index].append("hostname");
                    this.csvStringBuilders[index]
                            .append(this.csvOutputSeparator).append("pid");
                    this.csvStringBuilders[index]
                            .append(this.csvOutputSeparator).append("key");
                    this.csvStringBuilders[index]
                            .append(this.csvOutputSeparator);
                    OperationStatistics.getCsvHeaders(this.csvOutputSeparator,
                            this.csvStringBuilders[index]);

                    // we will receive writes to this file as well
                    this.csvWriters[index] = new BufferedWriter(
                            new FileWriter(file));
                    this.csvWriters[index]
                            .write(this.csvStringBuilders[index].toString());
                    this.csvWriters[index].newLine();

                    this.csvStringBuilders[index].setLength(0);
                } else {
                    throw new IOException(filename + " already exists");
                }
            }

            this.csvStringBuilders[index].append(this.systemHostname);
            this.csvStringBuilders[index].append(this.csvOutputSeparator)
                    .append(this.systemPid);
            this.csvStringBuilders[index].append(this.csvOutputSeparator)
                    .append(this.systemKey);
            this.csvStringBuilders[index].append(this.csvOutputSeparator);
            aggregate.toCsv(this.csvOutputSeparator,
                    this.csvStringBuilders[index]);

            this.csvWriters[index]
                    .write(this.csvStringBuilders[index].toString());
            this.csvWriters[index].newLine();
        }
    }

    @SuppressWarnings("resource") // we close the channels on shutdown
    private void writeBinary(OperationStatistics aggregate) throws IOException {
        int index = getUniqueIndex(aggregate.getSource(),
                aggregate.getCategory());
        synchronized (this.writerLocks[index]) {
            if (this.bbBuffers[index] == null) {
                switch (this.outputFormat) {
                case FB:
                    this.bbBuffers[index] = ByteBuffer.allocate(256);
                    break;
                case BB:
                    this.bbBuffers[index] = ByteBuffer.allocate(64);
                    break;
                case CSV:
                    // this should not happen
                default:
                    throw new IllegalArgumentException(
                            this.outputFormat.name());
                }
            } else {
                this.bbBuffers[index].clear();
            }

            if (this.bbChannels[index] == null) {
                String filename = getLogFilePrefix() + "."
                        + aggregate.getSource().name().toLowerCase() + "."
                        + aggregate.getCategory().name().toLowerCase() + "."
                        + this.initializationTime + "."
                        + this.outputFormat.name().toLowerCase();

                File file = new File(filename);
                if (!file.exists()) {
                    this.bbChannels[index] = new FileOutputStream(file)
                            .getChannel();
                } else {
                    throw new IOException(filename + " already exists");
                }
            }

            boolean overflow;
            do {
                overflow = false;
                try {
                    switch (this.outputFormat) {
                    case FB:
                        aggregate.toFlatBuffer(this.systemHostname,
                                this.systemPid, this.systemKey,
                                this.bbBuffers[index]);
                        // resulting buffer is already 'flipped'
                        break;
                    case BB:
                        aggregate.toByteBuffer(this.systemHostnameBb,
                                this.systemPid, this.systemKeyBb,
                                this.bbBuffers[index]);
                        this.bbBuffers[index].flip();
                        break;
                    case CSV:
                        // this should not happen
                    default:
                        throw new IllegalArgumentException(
                                this.outputFormat.name());
                    }
                } catch (BufferOverflowException e) {
                    overflow = true;
                    this.bbBuffers[index] = ByteBuffer.allocate(
                            (int) (this.bbBuffers[index].capacity() * 1.5));
                }
            } while (overflow);
            this.bbChannels[index].write(this.bbBuffers[index]);
        }
    }

    private void writeFileDescriptorMappings() {
        switch (this.outputFormat) {
        case CSV:
            writeFileDescriptorMappingsCsv();
            break;
        case FB:
            writeFileDescriptorMappingsFb();
            break;
        case BB:
            writeFileDescriptorMappingsBb();
            break;
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
        }
    }

    private void writeFileDescriptorMappingsCsv() {
        try {
            try (BufferedWriter fileDescriptorMappingsWriter = new BufferedWriter(
                    new FileWriter(new File(getLogFilePrefix()
                            + ".filedescriptormappings."
                            + this.initializationTime + "."
                            + this.outputFormat.name().toLowerCase())))) {

                fileDescriptorMappingsWriter.write("hostname");
                fileDescriptorMappingsWriter.write(this.csvOutputSeparator);
                fileDescriptorMappingsWriter.write("pid");
                fileDescriptorMappingsWriter.write(this.csvOutputSeparator);
                fileDescriptorMappingsWriter.write("key");
                fileDescriptorMappingsWriter.write(this.csvOutputSeparator);
                fileDescriptorMappingsWriter.write("fileDescriptor");
                fileDescriptorMappingsWriter.write(this.csvOutputSeparator);
                fileDescriptorMappingsWriter.write("filename");
                fileDescriptorMappingsWriter.newLine();

                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, Integer> fd : this.filenameToFd
                        .entrySet()) {
                    sb.append(this.systemHostname)
                            .append(this.csvOutputSeparator);
                    sb.append(this.systemPid).append(this.csvOutputSeparator);
                    sb.append(this.systemKey).append(this.csvOutputSeparator);
                    sb.append(fd.getValue()).append(this.csvOutputSeparator);
                    sb.append(fd.getKey());
                    fileDescriptorMappingsWriter.write(sb.toString());
                    fileDescriptorMappingsWriter.newLine();
                    sb.setLength(0);
                }

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeFileDescriptorMappingsFb() {
        try {
            @SuppressWarnings("resource") // we close the channel later on
            FileChannel fileDescriptorMappingsChannel = new FileOutputStream(
                    new File(getLogFilePrefix() + ".filedescriptormappings."
                            + this.initializationTime + "."
                            + this.outputFormat.name().toLowerCase()))
                                    .getChannel();

            FlatBufferBuilder builder = new FlatBufferBuilder(0);
            for (Map.Entry<String, Integer> fd : this.filenameToFd.entrySet()) {
                int hostnameOffset = builder.createString(this.systemHostname);
                int keyOffset = builder.createString(this.systemKey);
                int pathOffset = builder.createString(fd.getKey());
                FileDescriptorMappingFB.startFileDescriptorMappingFB(builder);
                FileDescriptorMappingFB.addHostname(builder, hostnameOffset);
                FileDescriptorMappingFB.addPid(builder, this.systemPid);
                FileDescriptorMappingFB.addKey(builder, keyOffset);
                FileDescriptorMappingFB.addFileDescriptor(builder,
                        fd.getValue());
                FileDescriptorMappingFB.addPath(builder, pathOffset);
                int fdm = FileDescriptorMappingFB
                        .endFileDescriptorMappingFB(builder);
                FileDescriptorMappingFB
                        .finishSizePrefixedFileDescriptorMappingFBBuffer(
                                builder, fdm);
                fileDescriptorMappingsChannel.write(builder.dataBuffer());
                builder.clear();
            }

            fileDescriptorMappingsChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeFileDescriptorMappingsBb() {
        try {
            @SuppressWarnings("resource") // we close the channel later on
            FileChannel fileDescriptorMappingsChannel = new FileOutputStream(
                    new File(getLogFilePrefix() + ".filedescriptormappings."
                            + this.initializationTime + "."
                            + this.outputFormat.name().toLowerCase()))
                                    .getChannel();

            ByteBuffer bb = ByteBuffer.allocate(1048576);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.mark();
            for (Map.Entry<String, Integer> fd : this.filenameToFd.entrySet()) {
                try {
                    FileDescriptorMappingBufferBuilder.serialize(fd.getValue(),
                            fd.getKey(), this.systemHostnameBb, this.systemPid,
                            this.systemKeyBb, bb);

                    // remember last good position
                    bb.mark();
                } catch (BufferOverflowException e) {
                    // reset to last good position
                    bb.reset();

                    // prepare for reading
                    bb.flip();

                    // flush buffer
                    fileDescriptorMappingsChannel.write(bb);

                    // empty and reset buffer
                    bb.clear();
                }
            }

            // write remains
            bb.flip();
            fileDescriptorMappingsChannel.write(bb);
            fileDescriptorMappingsChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
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
            default:
            }
            //$FALL-THROUGH$
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
            default:
            }
            //$FALL-THROUGH$
        default:
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
            this.aggregate = new ReadDataOperationStatistics(
                    LiveOperationStatisticsAggregator.this.timeBinDuration,
                    source, category, startTime, endTime, fd, data, isRemote);
        }

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime,
                int fd, long data) {
            this.aggregate = new DataOperationStatistics(
                    LiveOperationStatisticsAggregator.this.timeBinDuration,
                    source, category, startTime, endTime, fd, data);
        }

        public AggregationTask(OperationSource source,
                OperationCategory category, long startTime, long endTime,
                int fd) {
            this.aggregate = new OperationStatistics(
                    LiveOperationStatisticsAggregator.this.timeBinDuration,
                    source, category, startTime, endTime, fd);
        }

        @Override
        public Void getRawResult() {
            return null;
        }

        @Override
        protected void setRawResult(Void value) {
            // discard
        }

        @Override
        protected boolean exec() {
            while (this.aggregate != null) {
                // get the time bin applicable for this operation
                NavigableMap<Long, NavigableMap<Integer, OperationStatistics>> timeBins = LiveOperationStatisticsAggregator.this.aggregates
                        .get(getUniqueIndex(this.aggregate.getSource(),
                                this.aggregate.getCategory()));

                // get the file descriptor applicable for this operation
                NavigableMap<Integer, OperationStatistics> fileDescriptors = timeBins
                        .computeIfAbsent(this.aggregate.getTimeBin(),
                                l -> new ConcurrentSkipListMap<>());

                OperationStatistics aggregatedOperationStatistics = fileDescriptors
                        .merge(this.aggregate.getFileDescriptor(),
                                this.aggregate, (v1, v2) -> {
                                    try {
                                        return v1.aggregate(v2);
                                    } catch (OperationStatistics.NotAggregatableException e) {
                                        e.printStackTrace();
                                        throw new IllegalArgumentException(e);
                                    }
                                });
                aggregatedOperationStatistics.doAggregation();

                // make sure to emit aggregates when the cache is full until
                // it's half full again to avoid writing every time bin size
                // from now on, only have one thread do the emission check
                if (!EMISSION_IN_PROGRESS.getAndSet(true)) {
                    // emission was not in progress, all other threads now see
                    // it as in progress and skip this
                    int size = timeBins.size();
                    if (size > LiveOperationStatisticsAggregator.this.timeBinCacheSize) {
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
                            switch (LiveOperationStatisticsAggregator.this.outputFormat) {
                            case CSV:
                                for (Writer w : LiveOperationStatisticsAggregator.this.csvWriters)
                                    if (w != null)
                                        w.flush();
                                break;
                            case FB:
                            case BB:
                                for (FileChannel fc : LiveOperationStatisticsAggregator.this.bbChannels)
                                    if (fc != null)
                                        fc.force(false);
                                break;
                            default:
                                throw new IllegalArgumentException(
                                        LiveOperationStatisticsAggregator.this.outputFormat
                                                .name());
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // reset emission in progress state
                    EMISSION_IN_PROGRESS.set(false);
                }

                this.aggregate = LiveOperationStatisticsAggregator.this.overflowQueue
                        .poll();
            }

            return true;
        }
    }
}
