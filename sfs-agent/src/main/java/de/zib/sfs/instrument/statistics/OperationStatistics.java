/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

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
    protected static final int AGGREGATE_OFFSET = FILE_DESCRIPTOR_OFFSET + 4; // int
    protected static final int SIZE = AGGREGATE_OFFSET + 4;

    private static final int MAX_POOL_SIZE;
    static {
        // 2^29 - 1 is the most we can do, because we need the next two higher
        // bits
        int maxBytes = 536870911;
        MAX_POOL_SIZE = (maxBytes - (maxBytes % SIZE) - SIZE) / SIZE;
    }

    protected static final Object[] LOCK_CACHE;
    protected static final int LOCK_CACHE_SIZE;
    public static final AtomicLong lockWaitTime;
    static {
        int size = 1024;
        String sizeString = System.getProperty("de.zib.sfs.lockCache.os.size");
        if (sizeString != null) {
            try {
                size = Integer.parseInt(sizeString);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        LOCK_CACHE = new Object[LOCK_CACHE_SIZE = size];
        for (int i = 0; i < LOCK_CACHE_SIZE; ++i) {
            LOCK_CACHE[i] = new Object();
        }

        if (Globals.LOCK_DIAGNOSTICS) {
            lockWaitTime = new AtomicLong(0);
        } else {
            lockWaitTime = null;
        }
    }

    protected static final OperationStatistics[] impl = new OperationStatistics[3];

    private static ByteBufferFactory overflowByteBufferFactory;

    // OperationStatistics, DataOperationStatistics and
    // ReadDataOperationStatistics
    // top two bits (excluding sign bit) in each returned address determine
    // index into this array
    protected static final MemoryPool[] memory = new MemoryPool[3];

    // mask to extract the top two bits of each address
    protected static final int ADDRESS_MASK = 0b11 << 29;

    // offsets into the above array
    public static final int OS_OFFSET = 0, DOS_OFFSET = 1, RDOS_OFFSET = 2;

    public static MemoryPool getMemoryPool(int address) {
        return memory[(address & ADDRESS_MASK) >> 29];
    }

    public static OperationStatistics getImpl(int address) {
        return impl[(address & ADDRESS_MASK) >> 29];
    }

    public static int sanitizeAddress(int address) {
        return address & ~ADDRESS_MASK;
    }

    public static int getOperationStatisticsOffset(int address) {
        return (address & ADDRESS_MASK) >> 29;
    }

    public static int getOperationStatistics() {
        if (memory[OS_OFFSET] == null) {
            synchronized (OperationStatistics.class) {
                if (memory[OS_OFFSET] == null) {
                    memory[OS_OFFSET] = new MemoryPool(SIZE * MAX_POOL_SIZE,
                            SIZE);
                    impl[OS_OFFSET] = new OperationStatistics();
                }
            }
        }

        int address = memory[OS_OFFSET].alloc();
        return address | (OS_OFFSET << 29);
    }

    public static int getOperationStatistics(long count, long timeBin,
            long cpuTime, OperationSource source, OperationCategory category,
            int fd) {
        int address = getOperationStatistics();
        getOperationStatistics(getMemoryPool(address), sanitizeAddress(address),
                count, timeBin, cpuTime, source, category, fd);
        return address;
    }

    protected static void getOperationStatistics(MemoryPool mp, int address,
            long count, long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd) {
        setCount(mp, address, count);
        setTimeBin(mp, address, timeBin);
        setCpuTime(mp, address, cpuTime);
        setSource(mp, address, source);
        setCategory(mp, address, category);
        setFileDescriptor(mp, address, fd);
        mp.pool.putInt(address + AGGREGATE_OFFSET, -1);
    }

    public static int getOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, int fd) {
        return getOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd);
    }

    public static void returnOperationStatistics(int address) {
        getMemoryPool(address).free(sanitizeAddress(address));
    }

    public static long getCount(int address) {
        return getCount(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getCount(MemoryPool mp, int address) {
        return mp.pool.getLong(address + COUNT_OFFSET);
    }

    public static void setCount(int address, long count) {
        setCount(getMemoryPool(address), sanitizeAddress(address), count);
    }

    public static void setCount(MemoryPool mp, int address, long count) {
        mp.pool.putLong(address + COUNT_OFFSET, count);
    }

    public static void incrementCount(int address, long count) {
        incrementCount(getMemoryPool(address), sanitizeAddress(address), count);
    }

    public static void incrementCount(MemoryPool mp, int address, long count) {
        long current = mp.pool.getLong(address + COUNT_OFFSET);
        mp.pool.putLong(address + COUNT_OFFSET, current + count);
    }

    public static long getTimeBin(int address) {
        return getTimeBin(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getTimeBin(MemoryPool mp, int address) {
        return mp.pool.getLong(address + TIME_BIN_OFFSET);
    }

    public static void setTimeBin(int address, long timeBin) {
        setTimeBin(getMemoryPool(address), sanitizeAddress(address), timeBin);
    }

    public static void setTimeBin(MemoryPool mp, int address, long timeBin) {
        mp.pool.putLong(address + TIME_BIN_OFFSET, timeBin);
    }

    public static long getCpuTime(int address) {
        return getCpuTime(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getCpuTime(MemoryPool mp, int address) {
        return mp.pool.getLong(address + CPU_TIME_OFFSET);
    }

    public static void setCpuTime(int address, long cpuTime) {
        setCpuTime(getMemoryPool(address), sanitizeAddress(address), cpuTime);
    }

    public static void setCpuTime(MemoryPool mp, int address, long cpuTime) {
        mp.pool.putLong(address + CPU_TIME_OFFSET, cpuTime);
    }

    public static void incrementCpuTime(int address, long cpuTime) {
        incrementCpuTime(getMemoryPool(address), sanitizeAddress(address),
                cpuTime);
    }

    public static void incrementCpuTime(MemoryPool mp, int address,
            long cpuTime) {
        long current = mp.pool.getLong(address + CPU_TIME_OFFSET);
        mp.pool.putLong(address + CPU_TIME_OFFSET, current + cpuTime);
    }

    public static OperationSource getSource(int address) {
        return getSource(getMemoryPool(address), sanitizeAddress(address));
    }

    public static OperationSource getSource(MemoryPool mp, int address) {
        return OperationSource.VALUES[mp.pool.get(address + SOURCE_OFFSET)];
    }

    public static void setSource(int address, OperationSource source) {
        setSource(getMemoryPool(address), sanitizeAddress(address), source);
    }

    public static void setSource(MemoryPool mp, int address,
            OperationSource source) {
        mp.pool.put(address + SOURCE_OFFSET, (byte) source.ordinal());
    }

    public static OperationCategory getCategory(int address) {
        return getCategory(getMemoryPool(address), sanitizeAddress(address));
    }

    public static OperationCategory getCategory(MemoryPool mp, int address) {
        return OperationCategory.VALUES[mp.pool.get(address + CATEGORY_OFFSET)];
    }

    public static void setCategory(int address, OperationCategory category) {
        setCategory(getMemoryPool(address), sanitizeAddress(address), category);
    }

    public static void setCategory(MemoryPool mp, int address,
            OperationCategory category) {
        mp.pool.put(address + CATEGORY_OFFSET, (byte) category.ordinal());
    }

    public static int getFileDescriptor(int address) {
        return getFileDescriptor(getMemoryPool(address),
                sanitizeAddress(address));
    }

    public static int getFileDescriptor(MemoryPool mp, int address) {
        return mp.pool.getInt(address + FILE_DESCRIPTOR_OFFSET);
    }

    public static void setFileDescriptor(int address, int fd) {
        setFileDescriptor(getMemoryPool(address), sanitizeAddress(address), fd);
    }

    public static void setFileDescriptor(MemoryPool mp, int address, int fd) {
        mp.pool.putInt(address + FILE_DESCRIPTOR_OFFSET, fd);
    }

    public static int aggregate(int address, int other)
            throws NotAggregatableException {
        if (Globals.STRICT) {
            if ((address & ADDRESS_MASK) != (other & ADDRESS_MASK)) {
                throw new NotAggregatableException(
                        "Memory pools do not match: " + address + ", " + other);
            }
        }

        MemoryPool mp = getMemoryPool(address);
        int sanitizedAddress = sanitizeAddress(address);
        int sanitizedOther = sanitizeAddress(other);

        if (Globals.STRICT) {
            if (sanitizedAddress == sanitizedOther) {
                throw new NotAggregatableException("Cannot aggregate self");
            }

            long timeBin = getTimeBin(mp, sanitizedAddress);
            long sanitizedOtherTimeBin = getTimeBin(mp, sanitizedOther);
            if (sanitizedOtherTimeBin != timeBin) {
                throw new NotAggregatableException("Time bins do not match: "
                        + timeBin + ", " + sanitizedOtherTimeBin);
            }

            OperationSource source = getSource(mp, sanitizedAddress);
            OperationSource sanitizedOtherSource = getSource(mp,
                    sanitizedOther);
            if (!sanitizedOtherSource.equals(source)) {
                throw new NotAggregatableException("Sources do not match: "
                        + source + ", " + sanitizedOtherSource);
            }

            OperationCategory category = getCategory(mp, sanitizedAddress);
            OperationCategory sanitizedOtherCategory = getCategory(mp,
                    sanitizedOther);
            if (!sanitizedOtherCategory.equals(category)) {
                throw new NotAggregatableException("Categories do not match: "
                        + category + ", " + sanitizedOtherCategory);
            }

            int fileDescriptor = getFileDescriptor(mp, sanitizedAddress);
            int sanitizedOtherFileDescriptor = getFileDescriptor(mp,
                    sanitizedOther);
            if (sanitizedOtherFileDescriptor != fileDescriptor) {
                throw new NotAggregatableException(
                        "File descriptors do not match: " + fileDescriptor
                                + ", " + sanitizedOtherFileDescriptor);
            }
        }

        // set a reference to ourselves in the other OperationStatistcs
        mp.pool.putInt(sanitizedOther + AGGREGATE_OFFSET, sanitizedAddress);

        // return ourselves
        return address;
    }

    public static void doAggregation(int address) {
        getImpl(address).doAggregationImpl(getMemoryPool(address),
                sanitizeAddress(address));
    }

    protected void doAggregationImpl(MemoryPool mp, int address) {
        // if we do not hold an aggregate, bail out and don't free ourselves,
        // because we are the long-living instance
        int aggregate = mp.pool.getInt(address + AGGREGATE_OFFSET);
        if (aggregate < 0) {
            return;
        }

        // The JVM has an integer cache that could be used for locking as well,
        // but we use our own for custom locking. This introduces some
        // unnecessary synchronization between unrelated tasks, but hopefully
        // this is not too bad. aggregate is always a multiple of 2, so divide
        // by two to use full cache range.
        Object lock = LOCK_CACHE[(aggregate >> 1) % LOCK_CACHE_SIZE];

        long startWait;
        if (Globals.LOCK_DIAGNOSTICS) {
            startWait = System.currentTimeMillis();
        }
        synchronized (lock) {
            if (Globals.LOCK_DIAGNOSTICS) {
                lockWaitTime.addAndGet(System.currentTimeMillis() - startWait);
            }

            // add ourselves to the aggregate, then free ourselves because we
            // are the short-living instance
            incrementCount(mp, aggregate, getCount(mp, address));
            incrementCpuTime(mp, aggregate, getCpuTime(mp, address));
            mp.free(address);
        }
    }

    public static void getCsvHeaders(int address, String separator,
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
    }

    public static void toCsv(int address, String separator, StringBuilder sb) {
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
    }

    public static void fromCsv(String[] values, int off, int address) {
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
    }

    public static void toFlatBuffer(int address, String hostname, int pid,
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
    }

    public static void fromFlatBuffer(ByteBuffer buffer, int address) {
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
    }

    public static String toString(int address) {
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
}
