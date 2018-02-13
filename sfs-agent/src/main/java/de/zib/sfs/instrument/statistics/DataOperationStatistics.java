/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;
import de.zib.sfs.instrument.util.Globals;
import de.zib.sfs.instrument.util.MemoryPool;

public class DataOperationStatistics extends OperationStatistics {

    private static final int DATA_OFFSET = OperationStatistics.SIZE; // long
    protected static final int SIZE = DATA_OFFSET + 8;

    public static File mmapDirectory;

    private static int memoryCount = 0;

    private static final int POOL_SIZE = getPoolSize(
            "de.zib.sfs.dataOperationStatistics.poolSize", SIZE);
    public static final AtomicInteger maxPoolSize = Globals.POOL_DIAGNOSTICS
            ? new AtomicInteger(0)
            : null;

    public static long getDataOperationStatistics() {
        if (memoryCount == 0) {
            synchronized (DataOperationStatistics.class) {
                if (memoryCount == 0) {
                    impl[DOS_OFFSET] = new DataOperationStatistics();
                    memory.get(DOS_OFFSET).add(new MemoryPool(SIZE * POOL_SIZE,
                            SIZE, mmapDirectory));
                    memoryCount = 1;
                }
            }
        }

        int address = Integer.MIN_VALUE, listIndex = Integer.MIN_VALUE;
        final List<MemoryPool> memoryList = memory.get(DOS_OFFSET);
        for (int i = memoryCount - 1; i >= 0
                && address == Integer.MIN_VALUE; --i) {
            address = memoryList.get(i).alloc();
            listIndex = i;
        }

        if (address == Integer.MIN_VALUE) {
            MemoryPool mp = new MemoryPool(SIZE * POOL_SIZE, SIZE,
                    mmapDirectory);
            address = mp.alloc();
            synchronized (memoryList) {
                memoryList.add(mp);
                ++memoryCount;
                listIndex = memoryCount - 1;
                if (listIndex > MEMORY_POOL_MASK) {
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
            DataOperationStatistics.maxPoolSize
                    .updateAndGet((v) -> Math.max(v, maxPoolSize));
        }
        return address | ((long) DOS_OFFSET << 61) | ((long) listIndex << 32);
    }

    public static long getDataOperationStatistics(long count, long timeBin,
            long cpuTime, OperationSource source, OperationCategory category,
            int fd, long data) {
        long address = getDataOperationStatistics();
        getDataOperationStatistics(getMemoryPool(address),
                sanitizeAddress(address), count, timeBin, cpuTime, source,
                category, fd, data);
        return address;
    }

    protected static void getDataOperationStatistics(MemoryPool mp, int address,
            long count, long timeBin, long cpuTime, OperationSource source,
            OperationCategory category, int fd, long data) {
        OperationStatistics.getOperationStatistics(mp, address, count, timeBin,
                cpuTime, source, category, fd);
        setData(mp, address, data);
    }

    public static long getDataOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, int fd, long data) {
        return getDataOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data);
    }

    public static long getData(long address) {
        return getData(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getData(MemoryPool mp, int address) {
        return mp.pool.getLong(address + DATA_OFFSET);
    }

    public static void setData(long address, long data) {
        setData(getMemoryPool(address), sanitizeAddress(address), data);
    }

    public static void setData(MemoryPool mp, int address, long data) {
        mp.pool.putLong(address + DATA_OFFSET, data);
    }

    public static void incrementData(long address, long data) {
        incrementData(getMemoryPool(address), sanitizeAddress(address), data);
    }

    public static void incrementData(MemoryPool mp, int address, long data) {
        long current = mp.pool.getLong(address + DATA_OFFSET);
        mp.pool.putLong(address + DATA_OFFSET, current + data);
    }

    @Override
    protected void doAggregationImpl(MemoryPool mp, int address) {
        long aggregate = mp.pool.getLong(address + AGGREGATE_OFFSET);
        if (aggregate < 0) {
            return;
        }

        MemoryPool mpAggregate = getMemoryPool(aggregate);
        int sanitizedAggregate = sanitizeAddress(aggregate);

        // see super for reasoning behind locking mechanism
        Object lock = LOCK_CACHE[(sanitizedAggregate >> 1)
                & (LOCK_CACHE_SIZE - 1)];

        long startWait;
        if (Globals.LOCK_DIAGNOSTICS) {
            startWait = System.currentTimeMillis();
        }
        synchronized (lock) {
            if (Globals.LOCK_DIAGNOSTICS) {
                maxLockWaitTime.updateAndGet((v) -> Math.max(v,
                        System.currentTimeMillis() - startWait));
            }

            incrementData(mpAggregate, sanitizedAggregate,
                    getData(mp, address));
            super.doAggregationImpl(mp, address);
        }
    }

    @Override
    protected void getCsvHeadersImpl(MemoryPool mp, int address,
            String separator, StringBuilder sb) {
        super.getCsvHeadersImpl(mp, address, separator, sb);
        sb.append(separator).append("data");
    }

    @Override
    protected void toCsvImpl(MemoryPool mp, int address, String separator,
            StringBuilder sb) {
        super.toCsvImpl(mp, address, separator, sb);
        sb.append(separator).append(getData(mp, address));
    }

    @Override
    protected void fromCsvImpl(String[] values, int off, MemoryPool mp,
            int address) {
        super.fromCsvImpl(values, off, mp, address);
        setData(mp, address, Long.parseLong(values[off + 6]));
    }

    @Override
    protected void toFlatBufferImpl(MemoryPool mp, int address,
            FlatBufferBuilder builder) {
        super.toFlatBufferImpl(mp, address, builder);
        long data = getData(mp, address);
        if (data > 0)
            OperationStatisticsFB.addData(builder, data);
    }

    @Override
    protected void fromFlatBufferImpl(OperationStatisticsFB osfb, MemoryPool mp,
            int address) {
        super.fromFlatBufferImpl(osfb, mp, address);
        setData(mp, address, osfb.data());
    }

    public static void assertPoolEmpty() {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        int r = memory.get(DOS_OFFSET).stream().map((mp) -> mp.remaining())
                .reduce(0, (x, y) -> x + y);
        int p = memory.get(DOS_OFFSET).size() * POOL_SIZE;
        assert (r == p) : r + " actual vs. " + p + " expected";
    }
}
