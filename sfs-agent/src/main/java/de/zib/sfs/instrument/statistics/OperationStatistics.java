/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.io.File;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.flatbuffers.ByteBufferUtil;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FlatBufferBuilder.ByteBufferFactory;

import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;
import de.zib.sfs.instrument.util.Globals;
import de.zib.sfs.instrument.util.MemoryPool;

public class OperationStatistics {

    public static class NotAggregatableException extends Exception {
        private static final long serialVersionUID = 2284196048334825540L;

        public NotAggregatableException() {
            super();
        }

        public NotAggregatableException(String message) {
            super(message);
        }
    }

    protected static final int COUNT_OFFSET = 0; // long
    protected static final int TIME_BIN_OFFSET = COUNT_OFFSET + 8; // long
    protected static final int CPU_TIME_OFFSET = TIME_BIN_OFFSET + 8; // long
    protected static final int SOURCE_OFFSET = CPU_TIME_OFFSET + 8; // byte
    protected static final int CATEGORY_OFFSET = SOURCE_OFFSET + 1; // byte
    protected static final int FILE_DESCRIPTOR_OFFSET = CATEGORY_OFFSET + 1; // int
    protected static final int THREAD_ID_OFFSET = FILE_DESCRIPTOR_OFFSET + 4; // long
    protected static final int AGGREGATE_OFFSET = THREAD_ID_OFFSET + 8; // long
    protected static final int SIZE = AGGREGATE_OFFSET + 8;

    private static final int POOL_SIZE = getPoolSize(
            "de.zib.sfs.operationStatistics.poolSize", SIZE);
    public static final AtomicInteger maxPoolSize = Globals.POOL_DIAGNOSTICS
            ? new AtomicInteger(0)
            : null;

    protected static int getPoolSize(String key, int size) {
        // we can only allocate Integer.MAX_VALUE bytes in one pool
        int maxElements = (Integer.MAX_VALUE - (Integer.MAX_VALUE % size)
                - size) / size;
        String sizeString = System.getProperty(key);
        if (sizeString != null) {
            try {
                maxElements = Math.min(maxElements,
                        Integer.parseInt(sizeString));
            } catch (NumberFormatException e) {
                System.err
                        .println("Invalid number for " + key + ": " + sizeString
                                + ", falling back to " + maxElements + ".");
            }
        } else {
            // return somewhat reasonable (power of two) number of elements
            maxElements = 131072;
        }

        return maxElements;
    }

    public static File mmapDirectory;

    protected static final OperationStatistics[] impl = new OperationStatistics[3];

    private static ByteBufferFactory overflowByteBufferFactory;

    // OperationStatistics, DataOperationStatistics and
    // ReadDataOperationStatistics
    // top two bits (excluding sign bit) in each returned address determine
    // index into this list of lists
    protected static final List<List<MemoryPool>> memory = new ArrayList<>(3);
    static {
        memory.add(new ArrayList<>()); // OperationStatistics
        memory.add(new ArrayList<>()); // DataOperationStatistics
        memory.add(new ArrayList<>()); // ReadDataOperationStatistics
    }
    private static int memoryCount = 0;

    // mask to extract the top two bits of each address (excluding sign bit)
    protected static final long OS_OFFSET_MASK = 0x60000000L << 32;

    // offsets into the above array list
    public static final int OS_OFFSET = 0, DOS_OFFSET = 1, RDOS_OFFSET = 2;

    // mask to extract the next 29 bits of each address
    protected static final long MEMORY_POOL_MASK = 0x1FFFFFFFL << 32;

    public static MemoryPool getMemoryPool(long address) {
        return memory.get((int) ((address & OS_OFFSET_MASK) >> 61))
                .get((int) ((address & MEMORY_POOL_MASK) >> 32));
    }

    public static OperationStatistics getImpl(long address) {
        return impl[(int) ((address & OS_OFFSET_MASK) >> 61)];
    }

    public static int sanitizeAddress(long address) {
        // sanitized address within a memory pool is always an integer
        return (int) (address & 0xFFFFFFFFL);
    }

    public static int getOperationStatisticsOffset(long address) {
        return (int) ((address & OS_OFFSET_MASK) >> 61);
    }

    public static long getOperationStatistics() {
        if (memoryCount == 0) {
            synchronized (OperationStatistics.class) {
                if (memoryCount == 0) {
                    impl[OS_OFFSET] = new OperationStatistics();
                    memory.get(OS_OFFSET).add(new MemoryPool(SIZE * POOL_SIZE,
                            SIZE, mmapDirectory));
                    memoryCount = 1;
                }
            }
        }

        int address = Integer.MIN_VALUE, listIndex = Integer.MIN_VALUE;
        final List<MemoryPool> memoryList = memory.get(OS_OFFSET);
        for (int i = memoryCount - 1; i >= 0
                && address == Integer.MIN_VALUE; --i) {
            // returns Integer.MIN_VALUE if exhausted
            address = memoryList.get(i).alloc();
            listIndex = i;
        }

        // all pools are exhausted, append a new one
        if (address == Integer.MIN_VALUE) {
            // we may add multiple pools concurrently here
            MemoryPool mp = new MemoryPool(SIZE * POOL_SIZE, SIZE,
                    mmapDirectory);
            address = mp.alloc();
            synchronized (memoryList) {
                memoryList.add(mp);
                ++memoryCount;
                listIndex = memoryCount - 1;
                if (listIndex > MEMORY_POOL_MASK) {
                    // too many memory pools allocated
                    throw new OutOfMemoryError();
                }
            }
        }

        if (Globals.POOL_DIAGNOSTICS) {
            int mpr = 0;
            for (int i = 0; i < memoryCount; ++i) {
                mpr += POOL_SIZE - memoryList.get(i).remaining();
            }
            final int maxPoolSize = mpr;
            OperationStatistics.maxPoolSize
                    .updateAndGet((v) -> Math.max(v, maxPoolSize));
        }
        return address | ((long) OS_OFFSET << 61) | ((long) listIndex << 32);
    }

    public static long getOperationStatistics(long count, long timeBin,
            long cpuTime, OperationSource source, OperationCategory category,
            int fd, long threadId) {
        long address = getOperationStatistics();
        getOperationStatistics(getMemoryPool(address), sanitizeAddress(address),
                count, timeBin, cpuTime, source, category, fd, threadId);
        return address;
    }

    protected static void getOperationStatistics(MemoryPool mp, int address,
            long count, long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd, long threadId) {
        setCount(mp, address, count);
        setTimeBin(mp, address, timeBin);
        setCpuTime(mp, address, cpuTime);
        setSource(mp, address, source);
        setCategory(mp, address, category);
        setFileDescriptor(mp, address, fd);
        setThreadId(mp, address, threadId);
        mp.pool.putLong(address + AGGREGATE_OFFSET, -1);
    }

    public static long getOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, int fd, long threadId) {
        return getOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, threadId);
    }

    public static void returnOperationStatistics(long address) {
        getMemoryPool(address).free(sanitizeAddress(address));
    }

    public static long getCount(long address) {
        return getCount(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getCount(MemoryPool mp, int address) {
        return mp.pool.getLong(address + COUNT_OFFSET);
    }

    public static void setCount(long address, long count) {
        setCount(getMemoryPool(address), sanitizeAddress(address), count);
    }

    public static void setCount(MemoryPool mp, int address, long count) {
        mp.pool.putLong(address + COUNT_OFFSET, count);
    }

    public static void incrementCount(long address, long count) {
        incrementCount(getMemoryPool(address), sanitizeAddress(address), count);
    }

    public static void incrementCount(MemoryPool mp, int address, long count) {
        long current = mp.pool.getLong(address + COUNT_OFFSET);
        mp.pool.putLong(address + COUNT_OFFSET, current + count);
    }

    public static long getTimeBin(long address) {
        return getTimeBin(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getTimeBin(MemoryPool mp, int address) {
        return mp.pool.getLong(address + TIME_BIN_OFFSET);
    }

    public static void setTimeBin(long address, long timeBin) {
        setTimeBin(getMemoryPool(address), sanitizeAddress(address), timeBin);
    }

    public static void setTimeBin(MemoryPool mp, int address, long timeBin) {
        mp.pool.putLong(address + TIME_BIN_OFFSET, timeBin);
    }

    public static long getCpuTime(long address) {
        return getCpuTime(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getCpuTime(MemoryPool mp, int address) {
        return mp.pool.getLong(address + CPU_TIME_OFFSET);
    }

    public static void setCpuTime(long address, long cpuTime) {
        setCpuTime(getMemoryPool(address), sanitizeAddress(address), cpuTime);
    }

    public static void setCpuTime(MemoryPool mp, int address, long cpuTime) {
        mp.pool.putLong(address + CPU_TIME_OFFSET, cpuTime);
    }

    public static void incrementCpuTime(long address, long cpuTime) {
        incrementCpuTime(getMemoryPool(address), sanitizeAddress(address),
                cpuTime);
    }

    public static void incrementCpuTime(MemoryPool mp, int address,
            long cpuTime) {
        long current = mp.pool.getLong(address + CPU_TIME_OFFSET);
        mp.pool.putLong(address + CPU_TIME_OFFSET, current + cpuTime);
    }

    public static OperationSource getSource(long address) {
        return getSource(getMemoryPool(address), sanitizeAddress(address));
    }

    public static OperationSource getSource(MemoryPool mp, int address) {
        return OperationSource.VALUES[mp.pool.get(address + SOURCE_OFFSET)];
    }

    public static void setSource(long address, OperationSource source) {
        setSource(getMemoryPool(address), sanitizeAddress(address), source);
    }

    public static void setSource(MemoryPool mp, int address,
            OperationSource source) {
        mp.pool.put(address + SOURCE_OFFSET, (byte) source.ordinal());
    }

    public static OperationCategory getCategory(long address) {
        return getCategory(getMemoryPool(address), sanitizeAddress(address));
    }

    public static OperationCategory getCategory(MemoryPool mp, int address) {
        return OperationCategory.VALUES[mp.pool.get(address + CATEGORY_OFFSET)];
    }

    public static void setCategory(long address, OperationCategory category) {
        setCategory(getMemoryPool(address), sanitizeAddress(address), category);
    }

    public static void setCategory(MemoryPool mp, int address,
            OperationCategory category) {
        mp.pool.put(address + CATEGORY_OFFSET, (byte) category.ordinal());
    }

    public static int getFileDescriptor(long address) {
        return getFileDescriptor(getMemoryPool(address),
                sanitizeAddress(address));
    }

    public static int getFileDescriptor(MemoryPool mp, int address) {
        return mp.pool.getInt(address + FILE_DESCRIPTOR_OFFSET);
    }

    public static void setFileDescriptor(long address, int fd) {
        setFileDescriptor(getMemoryPool(address), sanitizeAddress(address), fd);
    }

    public static void setFileDescriptor(MemoryPool mp, int address, int fd) {
        mp.pool.putInt(address + FILE_DESCRIPTOR_OFFSET, fd);
    }

    public static long getThreadId(long address) {
        return getThreadId(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getThreadId(MemoryPool mp, int address) {
        return mp.pool.getLong(address + THREAD_ID_OFFSET);
    }

    public static void setThreadId(long address, long threadId) {
        setThreadId(getMemoryPool(address), sanitizeAddress(address), threadId);
    }

    public static void setThreadId(MemoryPool mp, int address, long threadId) {
        mp.pool.putLong(address + THREAD_ID_OFFSET, threadId);
    }

    public static long aggregate(long address, long other)
            throws NotAggregatableException {
        if (Globals.STRICT) {
            if ((address & OS_OFFSET_MASK) != (other & OS_OFFSET_MASK)) {
                throw new NotAggregatableException(
                        "Memory pools do not match: " + address + ", " + other);
            }
        }

        MemoryPool mpAddress = getMemoryPool(address);
        int sanitizedAddress = sanitizeAddress(address);
        MemoryPool mpOther = getMemoryPool(other);
        int sanitizedOther = sanitizeAddress(other);

        if (Globals.STRICT) {
            if (address == other) {
                throw new NotAggregatableException(
                        "Cannot aggregate self: " + address + ", " + other);
            }

            long threadId = getThreadId(mpAddress, sanitizedAddress);
            long sanitizedOtherThreadId = getThreadId(mpOther, sanitizedOther);
            if (sanitizedOtherThreadId != threadId) {
                throw new NotAggregatableException("Thread IDs do not match: "
                        + threadId + ", " + sanitizedOtherThreadId);
            }

            long timeBin = getTimeBin(mpAddress, sanitizedAddress);
            long sanitizedOtherTimeBin = getTimeBin(mpOther, sanitizedOther);
            if (sanitizedOtherTimeBin != timeBin) {
                throw new NotAggregatableException("Time bins do not match: "
                        + timeBin + ", " + sanitizedOtherTimeBin);
            }

            OperationSource source = getSource(mpAddress, sanitizedAddress);
            OperationSource sanitizedOtherSource = getSource(mpOther,
                    sanitizedOther);
            if (!sanitizedOtherSource.equals(source)) {
                throw new NotAggregatableException("Sources do not match: "
                        + source + ", " + sanitizedOtherSource);
            }

            OperationCategory category = getCategory(mpAddress,
                    sanitizedAddress);
            OperationCategory sanitizedOtherCategory = getCategory(mpOther,
                    sanitizedOther);
            if (!sanitizedOtherCategory.equals(category)) {
                throw new NotAggregatableException("Categories do not match: "
                        + category + ", " + sanitizedOtherCategory);
            }

            int fileDescriptor = getFileDescriptor(mpAddress, sanitizedAddress);
            int sanitizedOtherFileDescriptor = getFileDescriptor(mpOther,
                    sanitizedOther);
            if (sanitizedOtherFileDescriptor != fileDescriptor) {
                throw new NotAggregatableException(
                        "File descriptors do not match: " + fileDescriptor
                                + ", " + sanitizedOtherFileDescriptor);
            }
        }

        // set a reference to ourselves in the other OperationStatistcs
        mpOther.pool.putLong(sanitizedOther + AGGREGATE_OFFSET, address);

        // return ourselves
        return address;
    }

    public static void doAggregation(long address) {
        getImpl(address).doAggregationImpl(getMemoryPool(address),
                sanitizeAddress(address));
    }

    protected void doAggregationImpl(MemoryPool mp, int address) {
        // if we do not hold an aggregate, bail out and don't free ourselves,
        // because we are the long-living instance
        long aggregate = mp.pool.getLong(address + AGGREGATE_OFFSET);
        if (aggregate < 0) {
            return;
        }

        MemoryPool mpAggregate = getMemoryPool(aggregate);
        int sanitizedAggregate = sanitizeAddress(aggregate);

        // add ourselves to the aggregate, then free ourselves because we
        // are the short-living instance
        incrementCount(mpAggregate, sanitizedAggregate, getCount(mp, address));
        incrementCpuTime(mpAggregate, sanitizedAggregate,
                getCpuTime(mp, address));
        mp.free(address);
    }

    public static void getCsvHeaders(long address, String separator,
            StringBuilder sb) {
        getImpl(address).getCsvHeadersImpl(getMemoryPool(address),
                sanitizeAddress(address), separator, sb);
    }

    protected void getCsvHeadersImpl(MemoryPool mp, int address,
            String separator, StringBuilder sb) {
        sb.append("count");
        sb.append(separator).append("timeBin");
        sb.append(separator).append("cpuTime");
        sb.append(separator).append("source");
        sb.append(separator).append("category");
        sb.append(separator).append("fileDescriptor");
        sb.append(separator).append("threadId");
    }

    public static void toCsv(long address, String separator, StringBuilder sb) {
        getImpl(address).toCsvImpl(getMemoryPool(address),
                sanitizeAddress(address), separator, sb);
    }

    protected void toCsvImpl(MemoryPool mp, int address, String separator,
            StringBuilder sb) {
        sb.append(getCount(mp, address));
        sb.append(separator).append(getTimeBin(mp, address));
        sb.append(separator).append(getCpuTime(mp, address));
        sb.append(separator)
                .append(getSource(mp, address).name().toLowerCase());
        sb.append(separator)
                .append(getCategory(mp, address).name().toLowerCase());
        sb.append(separator).append(getFileDescriptor(mp, address));
        sb.append(separator).append(getThreadId(mp, address));
    }

    public static void fromCsv(String[] values, int off, long address) {
        getImpl(address).fromCsvImpl(values, off, getMemoryPool(address),
                sanitizeAddress(address));
    }

    protected void fromCsvImpl(String[] values, int off, MemoryPool mp,
            int address) {
        setCount(mp, address, Long.parseLong(values[off + 0]));
        setTimeBin(mp, address, Long.parseLong(values[off + 1]));
        setCpuTime(mp, address, Long.parseLong(values[off + 2]));
        setSource(mp, address,
                OperationSource.valueOf(values[off + 3].toUpperCase()));
        setCategory(mp, address,
                OperationCategory.valueOf(values[off + 4].toUpperCase()));
        setFileDescriptor(mp, address, Integer.parseInt(values[off + 5]));
        setThreadId(mp, address, Long.parseLong(values[off + 6]));
    }

    public static void toFlatBuffer(long address, String hostname, int pid,
            String key, ByteBuffer bb) {
        if (overflowByteBufferFactory == null) {
            overflowByteBufferFactory = new ByteBufferFactory() {
                @Override
                public ByteBuffer newByteBuffer(int capacity) {
                    // signal to the caller that the ByteBuffer was not
                    // sufficiently large
                    throw new BufferOverflowException();
                }
            };
        }

        FlatBufferBuilder builder = new FlatBufferBuilder(bb,
                overflowByteBufferFactory);

        int hostnameOffset = builder.createString(hostname);
        int keyOffset = builder.createString(key);
        OperationStatisticsFB.startOperationStatisticsFB(builder);
        OperationStatisticsFB.addHostname(builder, hostnameOffset);
        if (pid > 0)
            OperationStatisticsFB.addPid(builder, pid);
        OperationStatisticsFB.addKey(builder, keyOffset);

        getImpl(address).toFlatBufferImpl(getMemoryPool(address),
                sanitizeAddress(address), builder);

        int os = OperationStatisticsFB.endOperationStatisticsFB(builder);
        OperationStatisticsFB
                .finishSizePrefixedOperationStatisticsFBBuffer(builder, os);
    }

    protected void toFlatBufferImpl(MemoryPool mp, int address,
            FlatBufferBuilder builder) {
        long count = getCount(mp, address);
        if (count > 0)
            OperationStatisticsFB.addCount(builder, count);
        long timeBin = getTimeBin(mp, address);
        if (timeBin > 0)
            OperationStatisticsFB.addTimeBin(builder, timeBin);
        long cpuTime = getCpuTime(mp, address);
        if (cpuTime > 0)
            OperationStatisticsFB.addCpuTime(builder, cpuTime);
        OperationStatisticsFB.addSource(builder,
                getSource(mp, address).toFlatBuffer());
        OperationStatisticsFB.addCategory(builder,
                getCategory(mp, address).toFlatBuffer());
        int fileDescriptor = getFileDescriptor(mp, address);
        if (fileDescriptor > 0)
            OperationStatisticsFB.addFileDescriptor(builder, fileDescriptor);
        long threadId = getThreadId(mp, address);
        if (threadId > 0)
            OperationStatisticsFB.addThreadId(builder, threadId);
    }

    public static void fromFlatBuffer(ByteBuffer buffer, long address) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        int length = Constants.SIZE_PREFIX_LENGTH;
        if (buffer.remaining() < length)
            throw new BufferUnderflowException();
        length += ByteBufferUtil.getSizePrefix(buffer);
        if (buffer.remaining() < length)
            throw new BufferUnderflowException();
        ByteBuffer osBuffer = ByteBufferUtil.removeSizePrefix(buffer);
        OperationStatisticsFB osfb = OperationStatisticsFB
                .getRootAsOperationStatisticsFB(osBuffer);
        buffer.position(buffer.position() + length);

        getImpl(address).fromFlatBufferImpl(osfb, getMemoryPool(address),
                sanitizeAddress(address));
    }

    protected void fromFlatBufferImpl(OperationStatisticsFB osfb, MemoryPool mp,
            int address) {
        setCount(mp, address, osfb.count());
        setTimeBin(mp, address, osfb.timeBin());
        setCpuTime(mp, address, osfb.cpuTime());
        setSource(mp, address, OperationSource.fromFlatBuffer(osfb.source()));
        setCategory(mp, address,
                OperationCategory.fromFlatBuffer(osfb.category()));
        setFileDescriptor(mp, address, osfb.fileDescriptor());
        setThreadId(mp, address, osfb.threadId());
    }

    public static String toString(long address) {
        return getImpl(address).toString(getMemoryPool(address),
                sanitizeAddress(address));
    }

    private String toString(MemoryPool mp, int address) {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        toCsvImpl(mp, address, ",", sb);
        sb.append("}");
        return sb.toString();
    }

    public static void assertPoolEmpty() {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        int r = memory.get(OS_OFFSET).stream().map((mp) -> mp.remaining())
                .reduce(0, (x, y) -> x + y);
        int p = memory.get(OS_OFFSET).size() * POOL_SIZE;
        assert (r == p) : r + " actual vs. " + p + " expected";
    }
}
