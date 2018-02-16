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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.AbstractSfsCallback;
import de.zib.sfs.instrument.statistics.bb.FileDescriptorMappingBufferBuilder;
import de.zib.sfs.instrument.statistics.bb.OperationStatisticsBufferBuilder;
import de.zib.sfs.instrument.statistics.fb.FileDescriptorMappingFB;
import de.zib.sfs.instrument.util.ConcurrentIntLongSkipListMap;
import de.zib.sfs.instrument.util.ConcurrentLongObjectSkipListMap;
import de.zib.sfs.instrument.util.Globals;
import de.zib.sfs.instrument.util.LongQueue;
import de.zib.sfs.instrument.util.MemoryPool;

public class LiveOperationStatisticsAggregator {

    public static enum OutputFormat {
        CSV, FB, BB;
    }

    private static enum LifecyclePhase {
        UNINITIALIZED, INITIALIZING, RUNNING, SHUTTING_DOWN, SHUT_DOWN;
    }

    LifecyclePhase phase;

    private String systemHostname, systemKey;
    private ByteBuffer systemHostnameBb, systemKeyBb;
    private int systemPid;

    // needs to have a positive non-zero value before it has been properly
    // initialized, as it might be used before that
    long timeBinDuration = 1L;
    int timeBinCacheSize;

    OutputFormat outputFormat;

    private String logFilePrefix;

    // only have one thread per source/category emit from the aggregates
    final AtomicBoolean[] emissionInProgress;

    // per aggregation thread: for each source/category combination, map a time
    // bin to an aggregate operation statistics for each file
    final List<List<ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap>>> aggregates;

    // for coordinating writing of statistics
    private final Object[] writerLocks;

    // size in bytes of the output buffer to use
    private static final int OUTPUT_BUFFER_CAPACITY = 256 * 1024;

    // threshold in bytes after which the output buffer is spilled and cleared
    private static final int OUTPUT_BUFFER_SPILL_THRESHOLD = OUTPUT_BUFFER_CAPACITY
            - 1024;

    // for CSV output
    private ThreadLocal<StringBuilder> csvStringBuilder;
    BufferedWriter[] csvWriters;
    private String csvOutputSeparator;
    private String csvNewLine;

    // directory to be used for mmaping LongQueue and MemoryPool
    private File mmapDirectory;

    // CSV StringBuilder counts in chars, so divide by 2
    private static final int CSV_OUTPUT_BUFFER_SPILL_THRESHOLD = OUTPUT_BUFFER_SPILL_THRESHOLD >> 1;

    // for FlatBuffer/ByteBuffer output
    private ThreadLocal<ByteBuffer> bbBuffer;
    FileChannel[] bbChannels;

    // to keep track of the number of operation statistics
    private AtomicLong[] counters;

    private final int numAggregationThreads;
    private final ExecutorService threadPool;

    int taskQueueCount;
    final List<LongQueue> taskQueue;
    public static final AtomicInteger maxQueueSize = Globals.POOL_DIAGNOSTICS
            ? new AtomicInteger(0)
            : null;

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
        this.numAggregationThreads = Runtime.getRuntime().availableProcessors();
        final int numSourceCategories = OperationSource.VALUES.length
                * OperationCategory.VALUES.length;

        // each aggregation thread will have their own set of aggregates
        this.aggregates = new ArrayList<>(this.numAggregationThreads);
        for (int i = 0; i < this.numAggregationThreads; ++i) {
            // there are time bins for each source/category combination
            List<ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap>> timeBins = new ArrayList<>(
                    numSourceCategories);
            for (int j = 0; j < numSourceCategories; ++j) {
                timeBins.add(
                        new ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap>());
            }
            this.aggregates.add(timeBins);
        }

        // similar for the writer locks
        this.writerLocks = new Object[numSourceCategories];
        for (OperationSource source : OperationSource.VALUES) {
            for (OperationCategory category : OperationCategory.VALUES) {
                this.writerLocks[getUniqueIndex(source,
                        category)] = new Object();
            }
        }

        this.emissionInProgress = new AtomicBoolean[numSourceCategories];
        for (int i = 0; i < this.emissionInProgress.length; ++i) {
            this.emissionInProgress[i] = new AtomicBoolean(false);
        }

        this.taskQueue = new ArrayList<>();

        // worker pool that will accept the aggregation tasks
        this.threadPool = Executors
                .newFixedThreadPool(this.numAggregationThreads);

        this.filenameToFd = new ConcurrentHashMap<>();
        this.fdToFd = new ConcurrentHashMap<>();
        this.currentFileDescriptor = new AtomicInteger(0);

        this.phase = LifecyclePhase.UNINITIALIZED;
    }

    public void initialize() {
        synchronized (this) {
            if (!LifecyclePhase.UNINITIALIZED.equals(this.phase)) {
                return;
            }
            this.phase = LifecyclePhase.INITIALIZING;
        }

        this.systemHostname = System.getProperty("de.zib.sfs.hostname");

        this.systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        this.systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long
                .parseLong(System.getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer
                .parseInt(System.getProperty("de.zib.sfs.timeBin.cacheSize"));
        String outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");

        String mmapDir = System.getProperty("de.zib.sfs.mmap.directory");
        if (mmapDir != null && !"".equals(mmapDir = mmapDir.trim())) {
            this.mmapDirectory = new File(mmapDir);
            if (!this.mmapDirectory.exists()) {
                throw new IllegalArgumentException(
                        "mmap directory does not exist: " + mmapDir);
            } else if (!this.mmapDirectory.isDirectory()) {
                throw new IllegalArgumentException(
                        "mmap directory is not a directory: " + mmapDir);
            }

            // I know.
            OperationStatistics.mmapDirectory = this.mmapDirectory;
            DataOperationStatistics.mmapDirectory = this.mmapDirectory;
            ReadDataOperationStatistics.mmapDirectory = this.mmapDirectory;
        }

        // output format specifics
        this.outputFormat = OutputFormat.valueOf(
                System.getProperty("de.zib.sfs.output.format").toUpperCase());
        switch (this.outputFormat) {
        case CSV:
            this.csvOutputSeparator = ",";
            this.csvNewLine = System.lineSeparator();

            // guard against weird hostnames in CSV output
            this.systemHostname = this.systemHostname
                    .replaceAll(this.csvOutputSeparator, "");

            // StringBuilder capacity is in chars, so divide by 2
            this.csvStringBuilder = ThreadLocal.withInitial(
                    () -> new StringBuilder(OUTPUT_BUFFER_CAPACITY >> 1));
            this.csvWriters = new BufferedWriter[OperationSource.VALUES.length
                    * OperationCategory.VALUES.length];
            break;
        case FB:
        case BB:
            CharsetEncoder encoder = Charset.forName("US-ASCII").newEncoder();

            // pre-encode hostname
            if (this.systemHostname.length()
                    - Byte.MAX_VALUE > Byte.MAX_VALUE) {
                throw new IllegalArgumentException(this.systemHostname);
            }
            this.systemHostnameBb = ByteBuffer
                    .allocateDirect(this.systemHostname.length());
            encoder.reset();
            CoderResult cr = encoder.encode(
                    CharBuffer.wrap(this.systemHostname), this.systemHostnameBb,
                    true);
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
            this.systemKeyBb = ByteBuffer
                    .allocateDirect(this.systemKey.length());
            encoder.reset();
            cr = encoder.encode(CharBuffer.wrap(this.systemKey),
                    this.systemKeyBb, true);
            if (cr.isError()) {
                try {
                    cr.throwException();
                } catch (CharacterCodingException e) {
                    throw new IllegalArgumentException(this.systemKey, e);
                }
            }
            this.systemKeyBb.flip();

            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
            if (this.mmapDirectory == null) {
                this.bbBuffer = ThreadLocal.withInitial(() -> ByteBuffer
                        .allocateDirect(OUTPUT_BUFFER_CAPACITY));
            } else {
                // mmap the output buffer as well, since this may speed up
                // transfer to the output files
                final File md = this.mmapDirectory;
                this.bbBuffer = ThreadLocal.withInitial(() -> {
                    long id = Thread.currentThread().getId();
                    long time = System.currentTimeMillis();

                    File obFile = new File(md,
                            "outputbuffer-" + id + "-" + time + ".ob");
                    obFile.deleteOnExit();
                    ByteBuffer bb;
                    try {
                        RandomAccessFile obRaf = new RandomAccessFile(obFile,
                                "rw");
                        obRaf.setLength(OUTPUT_BUFFER_CAPACITY);
                        bb = obRaf.getChannel().map(MapMode.READ_WRITE, 0,
                                OUTPUT_BUFFER_CAPACITY);
                        obRaf.getChannel().close();
                        obRaf.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return bb;
                });
            }
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);

            this.bbChannels = new FileChannel[OperationSource.VALUES.length
                    * OperationCategory.VALUES.length];

            this.counters = new AtomicLong[OperationSource.VALUES.length
                    * OperationCategory.VALUES.length];
            for (int i = 0; i < this.counters.length; ++i) {
                this.counters[i] = new AtomicLong(0);
            }
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

        int queueSize = 1048576;
        String sizeString = System.getProperty("de.zib.sfs.queueSize");
        if (sizeString != null) {
            try {
                queueSize = Integer.parseInt(sizeString);
            } catch (NumberFormatException e) {
                System.err.println("Invalid number for de.zib.sfs.queueSize: "
                        + sizeString + ", falling back to " + queueSize + ".");
            }
        }
        this.taskQueue.add(new LongQueue(queueSize, this.mmapDirectory));
        this.taskQueueCount = 1;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LiveOperationStatisticsAggregator.this.shutdown();
            }
        });

        this.initializationTime = System.currentTimeMillis();
        this.phase = LifecyclePhase.RUNNING;

        for (int i = 0; i < this.numAggregationThreads; ++i) {
            this.threadPool
                    .submit(new AggregationTask(i, this.numAggregationThreads));
        }
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
        if (!LifecyclePhase.RUNNING.equals(this.phase) || filename == null
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
        if (!LifecyclePhase.RUNNING.equals(this.phase) || fileDescriptor == null
                || !this.traceFileDescriptors) {
            return 0;
        }

        return this.fdToFd.getOrDefault(fileDescriptor, 0);
    }

    public void aggregateOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd) {
        if (!LifecyclePhase.RUNNING.equals(this.phase)) {
            return;
        }

        // stretch request proportionally over time bins
        long startTimeBin = startTime - startTime % this.timeBinDuration;
        long endTimeBin = endTime - endTime % this.timeBinDuration;

        if (startTimeBin == endTimeBin) {
            long os = OperationStatistics.getOperationStatistics(1,
                    startTimeBin, endTime - startTime, source, category, fd);
            offerOperationStatistics(os);
        } else {
            // start from the last time bin and proceed to the second one
            for (long tb = endTimeBin; tb > startTimeBin; tb -= this.timeBinDuration) {
                // for tb > startTimeBin, endTime > tb
                long currentDuration = endTime - tb;

                // set endTime to the current timeBin
                endTime = tb;

                long os = OperationStatistics.getOperationStatistics(0, tb,
                        currentDuration, source, category, fd);
                offerOperationStatistics(os);
            }

            // only the first timeBin remains now
            long os = OperationStatistics.getOperationStatistics(1,
                    startTimeBin, endTime - startTime, source, category, fd);
            offerOperationStatistics(os);
        }

        if (Globals.POOL_DIAGNOSTICS) {
            LiveOperationStatisticsAggregator.maxQueueSize.updateAndGet(
                    (v) -> Math.max(v, remainingOperationStatistics()));
        }
        synchronized (this.taskQueue) {
            this.taskQueue.notify();
        }
    }

    public void aggregateDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data) {
        if (!LifecyclePhase.RUNNING.equals(this.phase)) {
            return;
        }

        long startTimeBin = startTime - startTime % this.timeBinDuration;
        long endTimeBin = endTime - endTime % this.timeBinDuration;
        long duration = endTime - startTime;

        if (startTimeBin == endTimeBin) {
            long dos = DataOperationStatistics.getDataOperationStatistics(1,
                    startTimeBin, duration, source, category, fd, data);
            offerOperationStatistics(dos);
        } else {
            long remainingData = data;
            for (long tb = endTimeBin; tb > startTimeBin; tb -= this.timeBinDuration) {
                long currentDuration = endTime - tb;
                endTime = tb;

                long currentData = (long) ((float) currentDuration / duration
                        * data);
                remainingData -= currentData;

                long dos = DataOperationStatistics.getDataOperationStatistics(0,
                        tb, currentDuration, source, category, fd, currentData);
                offerOperationStatistics(dos);
            }

            long dos = DataOperationStatistics.getDataOperationStatistics(1,
                    startTimeBin, endTime - startTime, source, category, fd,
                    remainingData);
            offerOperationStatistics(dos);
        }

        if (Globals.POOL_DIAGNOSTICS) {
            LiveOperationStatisticsAggregator.maxQueueSize.updateAndGet(
                    (v) -> Math.max(v, remainingOperationStatistics()));
        }
        synchronized (this.taskQueue) {
            this.taskQueue.notify();
        }
    }

    public void aggregateReadDataOperationStatistics(OperationSource source,
            OperationCategory category, long startTime, long endTime, int fd,
            long data, boolean isRemote) {
        if (!LifecyclePhase.RUNNING.equals(this.phase)) {
            return;
        }

        long startTimeBin = startTime - startTime % this.timeBinDuration;
        long endTimeBin = endTime - endTime % this.timeBinDuration;
        long duration = endTime - startTime;

        if (startTimeBin == endTimeBin) {
            long rdos = ReadDataOperationStatistics
                    .getReadDataOperationStatistics(1, startTimeBin, duration,
                            source, category, fd, data, isRemote ? 1 : 0,
                            isRemote ? duration : 0, isRemote ? data : 0);
            offerOperationStatistics(rdos);
        } else {
            long remainingData = data;
            for (long tb = endTimeBin; tb > startTimeBin; tb -= this.timeBinDuration) {
                long currentDuration = endTime - tb;
                endTime = tb;

                long currentData = (long) ((float) currentDuration / duration
                        * data);
                remainingData -= currentData;

                long rdos = ReadDataOperationStatistics
                        .getReadDataOperationStatistics(0, tb, currentDuration,
                                source, category, fd, currentData, 0,
                                isRemote ? currentDuration : 0,
                                isRemote ? currentData : 0);
                offerOperationStatistics(rdos);
            }

            long rdos = ReadDataOperationStatistics
                    .getReadDataOperationStatistics(1, startTimeBin,
                            endTime - startTime, source, category, fd,
                            remainingData, 0,
                            isRemote ? endTime - startTime : 0,
                            isRemote ? remainingData : 0);
            offerOperationStatistics(rdos);
        }

        if (Globals.POOL_DIAGNOSTICS) {
            LiveOperationStatisticsAggregator.maxQueueSize.updateAndGet(
                    (v) -> Math.max(v, remainingOperationStatistics()));
        }
        synchronized (this.taskQueue) {
            this.taskQueue.notify();
        }
    }

    private void offerOperationStatistics(long os) {
        boolean offered = false;
        for (int i = this.taskQueueCount - 1; i >= 0 && !offered; --i) {
            offered = this.taskQueue.get(i).offer(os);
        }
        if (!offered) {
            LongQueue lq = new LongQueue(this.taskQueue.get(0).capacity(),
                    this.mmapDirectory);
            lq.offer(os);
            synchronized (this.taskQueue) {
                this.taskQueue.add(lq);
                ++this.taskQueueCount;
            }
        }
    }

    long pollOperationStatistics() {
        long os = Long.MIN_VALUE;
        for (int i = 0; i < this.taskQueueCount && os == Long.MIN_VALUE; ++i) {
            os = this.taskQueue.get(i).poll();
        }
        return os;
    }

    private int remainingOperationStatistics() {
        int remaining = 0;
        for (int i = this.taskQueueCount - 1; i >= 0; --i) {
            remaining += this.taskQueue.get(i).remaining();
        }
        return remaining;
    }

    public synchronized void shutdown() {
        long shutdown;
        if (Globals.SHUTDOWN_DIAGNOSTICS) {
            shutdown = System.currentTimeMillis();
        }

        // synchronize the entire shutdown sequence in order to block concurrent
        // shutdown attempts, since they might send a SIGKILL if they return
        // early
        if (!LifecyclePhase.RUNNING.equals(this.phase)) {
            return;
        }
        this.phase = LifecyclePhase.SHUTTING_DOWN;

        this.threadPool.shutdown();

        // wake up all currently idle worker threads and have them discover that
        // we're shutting down so they empty the aggregates
        synchronized (this.taskQueue) {
            this.taskQueue.notifyAll();
        }

        // wait a bit for all still currently running tasks
        long threadPoolShutdown = 0;
        int taskQueueSize = 0;
        try {
            if (Globals.SHUTDOWN_DIAGNOSTICS) {
                taskQueueSize = remainingOperationStatistics();
                threadPoolShutdown = System.currentTimeMillis();
            }
            if (!this.threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("Thread pool did not shut down");
            }
            if (Globals.SHUTDOWN_DIAGNOSTICS) {
                threadPoolShutdown = System.currentTimeMillis()
                        - threadPoolShutdown;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

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
            ByteBuffer bbCount = ByteBuffer.allocate(8)
                    .order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; i < this.bbChannels.length; ++i) {
                FileChannel channel = this.bbChannels[i];
                if (channel != null)
                    try {
                        // prepend the number of elements in the channel
                        bbCount.putLong(0, this.counters[i].get());
                        channel.write(bbCount, 0L);
                        bbCount.position(0);

                        channel.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
            }
            break;
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
        }

        // write out the descriptor mappings
        long fdWait = 0;
        if (this.traceFileDescriptors) {
            if (Globals.SHUTDOWN_DIAGNOSTICS) {
                fdWait = System.currentTimeMillis();
            }
            writeFileDescriptorMappings();
            if (Globals.SHUTDOWN_DIAGNOSTICS) {
                fdWait = System.currentTimeMillis() - fdWait;
            }
        }

        if (Globals.LOCK_DIAGNOSTICS) {
            System.err.println("SFS Lock Diagnostics");
            System.err.println("  - LongQueue:           "
                    + LongQueue.maxLockWaitTime.get() + "ms");
            System.err.println("  - MemoryPool:          "
                    + MemoryPool.maxLockWaitTime.get() + "ms");
        }

        if (Globals.POOL_DIAGNOSTICS) {
            System.err.println("SFS Pool Diagnostics");
            System.err.println("  - OperationStatistics:         "
                    + OperationStatistics.maxPoolSize.get());
            System.err.println("  - DataOperationStatistics:     "
                    + DataOperationStatistics.maxPoolSize.get());
            System.err.println("  - ReadDataOperationStatistics: "
                    + ReadDataOperationStatistics.maxPoolSize.get());
            System.err.println(
                    "  - TaskQueue:                   " + maxQueueSize.get());
        }

        if (Globals.SHUTDOWN_DIAGNOSTICS) {
            System.err.println("SFS Shutdown Diagnostics");
            System.err.println("  - TaskQueue:        " + taskQueueSize);
            System.err.println(
                    "  - Thread pool:      " + threadPoolShutdown + "ms");
            System.err.println("  - File Descriptors: " + fdWait + "ms");
            System.err.println("  - Total:            "
                    + (System.currentTimeMillis() - shutdown) + "ms");
        }

        this.phase = LifecyclePhase.SHUT_DOWN;
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

    void write(ConcurrentIntLongSkipListMap.ValueIterator vi,
            OperationSource source, OperationCategory category, int index)
            throws IOException {
        switch (this.outputFormat) {
        case CSV:
            writeCsv(vi, source, category, index);
            break;
        case FB:
        case BB:
            writeBinary(vi, source, category, index);
            break;
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
        }
    }

    private void writeCsv(ConcurrentIntLongSkipListMap.ValueIterator vi,
            OperationSource source, OperationCategory category, int index)
            throws IOException {
        StringBuilder sb = this.csvStringBuilder.get();
        sb.setLength(0);

        boolean first = true;
        while (vi.hasNext()) {
            long aggregate = vi.next();

            // only check on first iteration
            if (first && this.csvWriters[index] == null) {
                synchronized (this.writerLocks[index]) {
                    // first thread to initialize writer also writes the headers
                    if (this.csvWriters[index] == null) {
                        String filename = getLogFilePrefix() + "."
                                + source.name().toLowerCase() + "."
                                + category.name().toLowerCase() + "."
                                + this.initializationTime + "."
                                + this.outputFormat.name().toLowerCase();

                        File file = new File(filename);
                        if (!file.exists()) {
                            sb.append("hostname");
                            sb.append(this.csvOutputSeparator).append("pid");
                            sb.append(this.csvOutputSeparator).append("key");
                            sb.append(this.csvOutputSeparator);
                            OperationStatistics.getCsvHeaders(aggregate,
                                    this.csvOutputSeparator, sb);
                            sb.append(this.csvNewLine);

                            // we will receive writes to this file as well in
                            // FileOutputStreamCallback
                            // bw is closed in shutdown
                            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
                            @SuppressWarnings("resource")
                            BufferedWriter bw = new BufferedWriter(
                                    new FileWriter(file));
                            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);

                            bw.write(sb.toString());
                            sb.setLength(0);

                            // this must be the last instruction in this
                            // synchronized block since other threads try to
                            // bail out early without obtaining the lock
                            this.csvWriters[index] = bw;
                        } else {
                            throw new IOException(filename + " already exists");
                        }
                    }
                }
            }

            // at this point, the writer is initialized and the headers have
            // been written
            first = false;

            // thread-local StringBuilder does not need to be synchronized
            sb.append(this.systemHostname);
            sb.append(this.csvOutputSeparator).append(this.systemPid);
            sb.append(this.csvOutputSeparator).append(this.systemKey);
            sb.append(this.csvOutputSeparator);
            OperationStatistics.toCsv(aggregate, this.csvOutputSeparator, sb);
            OperationStatistics.returnOperationStatistics(aggregate);
            sb.append(this.csvNewLine);

            // synchronized spill to source/category specific log file if the
            // buffer is full enough
            if (sb.length() >= CSV_OUTPUT_BUFFER_SPILL_THRESHOLD) {
                String csv = sb.toString();
                synchronized (this.writerLocks[index]) {
                    this.csvWriters[index].write(csv);
                }
                sb.setLength(0);
            }
        }

        // write remains
        if (sb.length() > 0) {
            String csv = sb.toString();
            synchronized (this.writerLocks[index]) {
                this.csvWriters[index].write(csv);
            }
        }
    }

    @SuppressWarnings("resource") // we close the channels on shutdown
    private void writeBinary(ConcurrentIntLongSkipListMap.ValueIterator vi,
            OperationSource source, OperationCategory category, int index)
            throws IOException {
        /* approach similar to writeCsv, see over there for comments */

        ByteBuffer bb = this.bbBuffer.get();
        bb.clear();

        long count = 0;
        while (vi.hasNext()) {
            long aggregate = vi.next();

            if (count == 0 && this.bbChannels[index] == null) {
                synchronized (this.writerLocks[index]) {
                    if (this.bbChannels[index] == null) {
                        String filename = getLogFilePrefix() + "."
                                + source.name().toLowerCase() + "."
                                + category.name().toLowerCase() + "."
                                + this.initializationTime + "."
                                + this.outputFormat.name().toLowerCase();

                        File file = new File(filename);
                        if (!file.exists()) {
                            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
                            FileChannel fc = new FileOutputStream(file)
                                    .getChannel();

                            // leave space for prepending the number of elements
                            // later
                            ByteBuffer bbCount = ByteBuffer.allocate(8)
                                    .order(ByteOrder.LITTLE_ENDIAN);
                            bbCount.putLong(0, 0L);
                            fc.write(bbCount);
                            bbCount = null;

                            this.bbChannels[index] = fc;
                            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);
                        } else {
                            throw new IOException(filename + " already exists");
                        }
                    }
                }
            }

            ++count;

            boolean spill = false;
            switch (this.outputFormat) {
            case FB:
                // FlatBuffers are built from the back, so create a slice with
                // slice.capacity() == bb.remaining()
                ByteBuffer slice = bb.slice(); // implicit new: bad!

                // after this, slice's position points to the start of
                // aggregate, so slice.remaining() is aggregate's size in slice
                OperationStatistics.toFlatBuffer(aggregate, this.systemHostname,
                        this.systemPid, this.systemKey, slice);
                OperationStatistics.returnOperationStatistics(aggregate);

                // decrease bb's limit by aggregate's size to reduce
                // bb.capacity() while leaving its position at 0
                bb.limit(bb.limit() - slice.remaining());

                // capacity minus limit now indicates the total amount of data
                // written to bb
                if ((spill = bb.capacity() - bb
                        .limit() >= OUTPUT_BUFFER_SPILL_THRESHOLD) == true) {
                    // custom 'flip' for reading
                    bb.position(bb.limit());
                    bb.limit(bb.capacity());
                }
                break;
            case BB:
                OperationStatisticsBufferBuilder.serialize(
                        this.systemHostnameBb, this.systemPid, this.systemKeyBb,
                        aggregate, bb);
                OperationStatistics.returnOperationStatistics(aggregate);

                if ((spill = bb
                        .position() >= OUTPUT_BUFFER_SPILL_THRESHOLD) == true) {
                    // flip for reading
                    bb.flip();
                }
                break;
            case CSV:
                // this should not happen
            default:
                throw new IllegalArgumentException(this.outputFormat.name());
            }

            if (spill) {
                // bb is already flipped
                synchronized (this.writerLocks[index]) {
                    this.bbChannels[index].write(bb);
                }
                bb.clear();
            }
        }

        this.counters[index].addAndGet(count);

        switch (this.outputFormat) {
        case FB:
            if (bb.capacity() - bb.limit() > 0) {
                bb.position(bb.limit());
                bb.limit(bb.capacity());
                synchronized (this.writerLocks[index]) {
                    this.bbChannels[index].write(bb);
                }
            }
            break;
        case BB:
            if (bb.position() > 0) {
                bb.flip();
                synchronized (this.writerLocks[index]) {
                    this.bbChannels[index].write(bb);
                }
            }
            break;
        case CSV:
            // this should not happen
        default:
            throw new IllegalArgumentException(this.outputFormat.name());
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
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
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
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeFileDescriptorMappingsFb() {
        try {
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
            @SuppressWarnings("resource") // we close the channel later on
            FileChannel fileDescriptorMappingsChannel = new FileOutputStream(
                    new File(getLogFilePrefix() + ".filedescriptormappings."
                            + this.initializationTime + "."
                            + this.outputFormat.name().toLowerCase()))
                                    .getChannel();
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);

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
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
            @SuppressWarnings("resource") // we close the channel later on
            FileChannel fileDescriptorMappingsChannel = new FileOutputStream(
                    new File(getLogFilePrefix() + ".filedescriptormappings."
                            + this.initializationTime + "."
                            + this.outputFormat.name().toLowerCase()))
                                    .getChannel();
            AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);

            ByteBuffer bb = this.bbBuffer.get();
            bb.clear();
            bb.order(ByteOrder.LITTLE_ENDIAN);

            // prepend the number of file descriptor mappings
            Set<Map.Entry<String, Integer>> fileDescriptorMappings = this.filenameToFd
                    .entrySet();
            bb.putInt(fileDescriptorMappings.size());

            for (Map.Entry<String, Integer> fd : fileDescriptorMappings) {
                FileDescriptorMappingBufferBuilder.serialize(fd.getValue(),
                        fd.getKey(), this.systemHostnameBb, this.systemPid,
                        this.systemKeyBb, bb);

                // buffer reached write threshold
                if (bb.position() >= OUTPUT_BUFFER_SPILL_THRESHOLD) {
                    bb.flip();
                    fileDescriptorMappingsChannel.write(bb);
                    bb.clear();
                }
            }

            // write remains
            if (bb.position() > 0) {
                bb.flip();
                fileDescriptorMappingsChannel.write(bb);
            }

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
            case ZIP:
                return 7;
            default:
                throw new IllegalArgumentException(category.name());
            }
        default:
            throw new IllegalArgumentException(source.name());
        }
    }

    static OperationSource getSource(int index) {
        switch (index) {
        case 0:
        case 1:
        case 2:
        case 6:
            return OperationSource.JVM;
        case 3:
        case 4:
        case 5:
        case 7:
            return OperationSource.SFS;
        default:
            throw new IllegalArgumentException(Integer.toString(index));
        }
    }

    static OperationCategory getCategory(int index) {
        switch (index) {
        case 0:
        case 3:
            return OperationCategory.READ;
        case 1:
        case 4:
            return OperationCategory.WRITE;
        case 2:
        case 5:
            return OperationCategory.OTHER;
        case 6:
        case 7:
            return OperationCategory.ZIP;
        default:
            throw new IllegalArgumentException(Integer.toString(index));
        }
    }

    private class AggregationTask implements Runnable {
        private final int aggregationTaskIndex, numAggregationTasks;

        public AggregationTask(int aggregateIndex, int numAggregationTasks) {
            this.aggregationTaskIndex = aggregateIndex;
            this.numAggregationTasks = numAggregationTasks;
        }

        private int doAggregation(long aggregate) {
            MemoryPool mp = OperationStatistics.getMemoryPool(aggregate);
            int aggregateAddress = OperationStatistics
                    .sanitizeAddress(aggregate);

            // get the time bin applicable for this operation for this
            // thread
            OperationSource source = OperationStatistics.getSource(mp,
                    aggregateAddress);
            OperationCategory category = OperationStatistics.getCategory(mp,
                    aggregateAddress);
            int index = getUniqueIndex(source, category);
            ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap> timeBins = LiveOperationStatisticsAggregator.this.aggregates
                    .get(this.aggregationTaskIndex).get(index);

            // get the file descriptor applicable for this operation
            ConcurrentIntLongSkipListMap fileDescriptors = timeBins
                    .computeIfAbsent(
                            OperationStatistics.getTimeBin(mp,
                                    aggregateAddress),
                            l -> new ConcurrentIntLongSkipListMap());

            fileDescriptors.merge(
                    OperationStatistics.getFileDescriptor(mp, aggregateAddress),
                    aggregate, (v1, v2) -> {
                        try {
                            // sets a reference to v1 in v2 and returns v1
                            return OperationStatistics.aggregate(v1, v2);
                        } catch (OperationStatistics.NotAggregatableException e) {
                            e.printStackTrace();
                            throw new IllegalArgumentException(e);
                        }
                    });

            // Upon successful merge, this.aggregate holds a reference
            // to the OperationStatistics that was already in the map.
            // If there was no OperationStatistics in the map before,
            // the reference will be empty. So trigger the aggregation
            // on this.aggregate.
            OperationStatistics.doAggregation(aggregate);

            // return the calculated source/category index for convenience
            return index;
        }

        @Override
        public void run() {
            while (LifecyclePhase.RUNNING
                    .equals(LiveOperationStatisticsAggregator.this.phase)
                    || remainingOperationStatistics() > 0) {
                long aggregate = pollOperationStatistics();
                if (aggregate == Long.MIN_VALUE) {
                    // nothing to do at the moment, wait on the queue to save
                    // CPU cycles if we're not shutting down at the moment
                    if (LifecyclePhase.RUNNING.equals(
                            LiveOperationStatisticsAggregator.this.phase)) {
                        synchronized (LiveOperationStatisticsAggregator.this.taskQueue) {
                            try {
                                LiveOperationStatisticsAggregator.this.taskQueue
                                        .wait();
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                    }
                    continue;
                }

                // do the actual aggregation and obtain the unique
                // source/category index
                int index = doAggregation(aggregate);

                // make sure to emit aggregates when the cache is full until
                // it's half full again to avoid writing every time bin size
                // from now on, only have one thread do the emission check per
                // source/category
                if (!LiveOperationStatisticsAggregator.this.emissionInProgress[index]
                        .getAndSet(true)) {
                    ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap> timeBins = LiveOperationStatisticsAggregator.this.aggregates
                            .get(this.aggregationTaskIndex).get(index);

                    // emission was not in progress, all other threads now see
                    // it as in progress and skip this
                    int size = timeBins.size();
                    if (size > LiveOperationStatisticsAggregator.this.timeBinCacheSize) {
                        // for this aggregation thread, the emission threshold
                        // has been reached, so assume the same for the other
                        // threads and poll their aggregates as well,
                        // aggregating them before emission
                        for (int aggregationTask = 0; aggregationTask < this.numAggregationTasks; ++aggregationTask) {
                            if (aggregationTask == this.aggregationTaskIndex) {
                                // skip this thread since we will use it as
                                // final aggregation target
                                continue;
                            }

                            // get the time bins for the other aggregation task,
                            // poll half of them and aggregate them in this
                            // thread
                            ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap> foreignTimeBins = LiveOperationStatisticsAggregator.this.aggregates
                                    .get(aggregationTask).get(index);
                            for (int i = size / 2; i > 0; --i) {
                                ConcurrentIntLongSkipListMap foreignFds = foreignTimeBins
                                        .poll();
                                if (foreignFds != null) {
                                    ConcurrentIntLongSkipListMap.ValueIterator vi = foreignFds
                                            .values();
                                    while (vi.hasNext()) {
                                        doAggregation(vi.next());
                                    }
                                } else {
                                    break;
                                }
                            }
                        }

                        for (int i = size / 2; i > 0; --i) {
                            try {
                                ConcurrentIntLongSkipListMap fds = timeBins
                                        .poll();
                                if (fds != null) {
                                    write(fds.values(), getSource(index),
                                            getCategory(index), index);
                                } else {
                                    break;
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    // reset emission in progress state
                    LiveOperationStatisticsAggregator.this.emissionInProgress[index]
                            .set(false);
                }
            }

            // taskQueue is empty and we're shutting down, so write the
            // remaining aggregates
            for (int index = 0; index < OperationSource.VALUES.length
                    * OperationCategory.VALUES.length; ++index) {
                OperationSource source = getSource(index);
                OperationCategory category = getCategory(index);

                // see above
                if (!LiveOperationStatisticsAggregator.this.emissionInProgress[index]
                        .getAndSet(true)) {
                    ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap> timeBins = LiveOperationStatisticsAggregator.this.aggregates
                            .get(this.aggregationTaskIndex).get(index);

                    for (int aggregationTask = 0; aggregationTask < this.numAggregationTasks; ++aggregationTask) {
                        if (aggregationTask == this.aggregationTaskIndex) {
                            continue;
                        }

                        ConcurrentLongObjectSkipListMap<ConcurrentIntLongSkipListMap> foreignTimeBins = LiveOperationStatisticsAggregator.this.aggregates
                                .get(aggregationTask).get(index);
                        int size = foreignTimeBins.size();
                        for (int i = 0; i < size; ++i) {
                            ConcurrentIntLongSkipListMap foreignFds = foreignTimeBins
                                    .poll();
                            if (foreignFds != null) {
                                ConcurrentIntLongSkipListMap.ValueIterator vi = foreignFds
                                        .values();
                                while (vi.hasNext()) {
                                    doAggregation(vi.next());
                                }
                            } else {
                                break;
                            }
                        }
                    }

                    ConcurrentIntLongSkipListMap fileDescriptors;
                    while ((fileDescriptors = timeBins.poll()) != null) {
                        // fileDescriptors is exclusive to this thread, so it's
                        // safe to iterate over the values
                        try {
                            write(fileDescriptors.values(), source, category,
                                    index);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    // do not reset emission state to prevent other threads from
                    // attempting to emit this source/category index again
                }
            }
        }
    }

    public void assertQueueEmpty() {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        int r = remainingOperationStatistics();
        assert (r == 0) : r + " actual vs. 0 expected";
    }
}
