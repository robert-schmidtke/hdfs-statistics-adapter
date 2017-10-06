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

import de.zib.sfs.instrument.statistics.bb.ByteBufferUtil;
import de.zib.sfs.instrument.statistics.bb.ByteBufferUtil.NumberType;

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

    public String getHostname() {
        return systemHostname;
    }

    public int getPid() {
        return systemPid;
    }

    public String getKey() {
        return systemKey;
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
            writeFileDescriptorMappings();
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

    private void writeFileDescriptorMappings() {
        switch (outputFormat) {
        case FB:
            System.err.println(
                    "FlatBuffer output for file descriptor mappings not yet supported, "
                            + "falling back to CSV.");
        case CSV:
            writeFileDescriptorMappingsCsv();
            break;
        case BB:
            writeFileDescriptorMappingsBinary();
            break;
        default:
            throw new IllegalArgumentException(outputFormat.name());
        }
    }

    private void writeFileDescriptorMappingsCsv() {
        try {
            BufferedWriter fileDescriptorMappingsWriter = new BufferedWriter(
                    new FileWriter(new File(getLogFilePrefix()
                            + ".filedescriptormappings." + initializationTime
                            + outputFormat.name().toLowerCase())));

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
            for (Map.Entry<String, Integer> fd : fileDescriptors.entrySet()) {
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

    private void writeFileDescriptorMappingsBinary() {
        CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();

        try {
            @SuppressWarnings("resource") // we close the channel later on
            FileChannel fileDescriptorMappingsChannel = new FileOutputStream(
                    new File(getLogFilePrefix() + ".filedescriptormappings."
                            + initializationTime + "."
                            + outputFormat.name().toLowerCase())).getChannel();

            // pre-encode hostname and key
            String[] preEncodeStrings = new String[] { systemHostname,
                    systemKey };
            int[] preEncodeLengths = new int[] { systemHostname.length(),
                    systemKey.length() };
            ByteBuffer[] preEncodeBbs = new ByteBuffer[preEncodeStrings.length];
            for (int i = 0; i < preEncodeStrings.length; ++i) {
                String s = preEncodeStrings[i];
                if (preEncodeLengths[i] - Byte.MAX_VALUE > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException(s);
                }

                preEncodeBbs[i] = ByteBuffer.allocate(preEncodeLengths[i]);
                encoder.reset();
                CoderResult cr = encoder.encode(CharBuffer.wrap(s),
                        preEncodeBbs[i], true);
                if (cr.isError()) {
                    try {
                        cr.throwException();
                    } catch (CharacterCodingException e) {
                        throw new IllegalArgumentException(s, e);
                    }
                }
                preEncodeBbs[i].flip();
                preEncodeBbs[i].mark();
            }

            ByteBuffer bb = ByteBuffer.allocate(1048576);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            bb.mark();
            for (Map.Entry<String, Integer> fd : fileDescriptors.entrySet()) {
                try {
                    int fileDescriptor = fd.getValue();
                    String path = fd.getKey();

                    // header byte:
                    // 0-1: empty
                    // 2-3: pidType
                    // 4-5: fdType
                    // 6-7: lengthType
                    NumberType ntPid = ByteBufferUtil.getNumberType(systemPid);
                    NumberType ntFd = ByteBufferUtil
                            .getNumberType(fileDescriptor);
                    NumberType ntLength = ByteBufferUtil
                            .getNumberType(path.length());
                    byte header = (byte) ((ntPid.ordinal() << 4)
                            | (ntFd.ordinal() << 2) | ntLength.ordinal());
                    bb.put(header);

                    // hostname
                    bb.put((byte) (preEncodeLengths[0] - Byte.MAX_VALUE))
                            .put(preEncodeBbs[0]);
                    preEncodeBbs[0].reset();

                    // pid
                    ntPid.putInt(bb, systemPid);

                    // key
                    bb.put((byte) (preEncodeLengths[1] - Byte.MAX_VALUE))
                            .put(preEncodeBbs[1]);
                    preEncodeBbs[1].reset();

                    // file descriptor
                    ntFd.putInt(bb, fileDescriptor);

                    // path
                    ntLength.putInt(bb, path.length());
                    if (path.length() > bb.remaining()) {
                        throw new BufferOverflowException();
                    }
                    encoder.reset();
                    CoderResult cr = encoder.encode(CharBuffer.wrap(path), bb,
                            true);
                    if (cr.isError()) {
                        try {
                            cr.throwException();
                        } catch (CharacterCodingException e) {
                            throw new IllegalArgumentException(path, e);
                        }
                    }

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
