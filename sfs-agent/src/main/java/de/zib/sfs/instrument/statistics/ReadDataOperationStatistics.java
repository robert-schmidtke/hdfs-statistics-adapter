/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.util.concurrent.atomic.AtomicInteger;

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;
import de.zib.sfs.instrument.util.Globals;
import de.zib.sfs.instrument.util.MemoryPool;

public class ReadDataOperationStatistics extends DataOperationStatistics {

    private static final int REMOTE_COUNT_OFFSET = DataOperationStatistics.SIZE; // long
    private static final int REMOTE_CPU_TIME_OFFSET = REMOTE_COUNT_OFFSET + 8; // long
    private static final int REMOTE_DATA_OFFSET = REMOTE_CPU_TIME_OFFSET + 8; // long
    static final int SIZE = REMOTE_DATA_OFFSET + 8;

    private static final int POOL_SIZE = getPoolSize(
            "de.zib.sfs.readDataOperationStatistics.poolSize", SIZE);
    public static final AtomicInteger maxPoolSize = Globals.POOL_DIAGNOSTICS
            ? new AtomicInteger(0)
            : null;

    public static long getReadDataOperationStatistics() {
        if (memory[RDOS_OFFSET] == null) {
            synchronized (ReadDataOperationStatistics.class) {
                if (memory[RDOS_OFFSET] == null) {
                    memory[RDOS_OFFSET] = new MemoryPool(SIZE * POOL_SIZE,
                            SIZE);
                    impl[RDOS_OFFSET] = new ReadDataOperationStatistics();
                }
            }
        }

        int address = memory[RDOS_OFFSET].alloc();
        if (Globals.POOL_DIAGNOSTICS) {
            maxPoolSize.updateAndGet((v) -> Math.max(v,
                    POOL_SIZE - memory[RDOS_OFFSET].remaining()));
        }
        return address | ((long) RDOS_OFFSET << 61);
    }

    public static long getReadDataOperationStatistics(long count, long timeBin,
            long cpuTime, OperationSource source, OperationCategory category,
            int fd, long data, long remoteCount, long remoteCpuTime,
            long remoteData) {
        long address = getReadDataOperationStatistics();
        getReadDataOperationStatistics(getMemoryPool(address),
                sanitizeAddress(address), count, timeBin, cpuTime, source,
                category, fd, data, remoteCount, remoteCpuTime, remoteData);
        return address;
    }

    protected static void getReadDataOperationStatistics(MemoryPool mp,
            int address, long count, long timeBin, long cpuTime,
            OperationSource source, OperationCategory category, int fd,
            long data, long remoteCount, long remoteCpuTime, long remoteData) {
        DataOperationStatistics.getDataOperationStatistics(mp, address, count,
                timeBin, cpuTime, source, category, fd, data);
        setRemoteCount(mp, address, remoteCount);
        setRemoteCpuTime(mp, address, remoteCpuTime);
        setRemoteData(mp, address, remoteData);
    }

    public static long getReadDataOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, int fd, long data, boolean isRemote) {
        return getReadDataOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data, isRemote ? 1 : 0,
                isRemote ? endTime - startTime : 0, isRemote ? data : 0);
    }

    public static long getRemoteCount(long address) {
        return getRemoteCount(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getRemoteCount(MemoryPool mp, int address) {
        return mp.pool.getLong(address + REMOTE_COUNT_OFFSET);
    }

    public static void setRemoteCount(long address, long remoteCount) {
        setRemoteCount(getMemoryPool(address), sanitizeAddress(address),
                remoteCount);
    }

    public static void setRemoteCount(MemoryPool mp, int address,
            long remoteCount) {
        mp.pool.putLong(address + REMOTE_COUNT_OFFSET, remoteCount);
    }

    public static void incrementRemoteCount(long address, long remoteCount) {
        incrementRemoteCount(getMemoryPool(address), sanitizeAddress(address),
                remoteCount);
    }

    public static void incrementRemoteCount(MemoryPool mp, int address,
            long remoteCount) {
        long current = mp.pool.getLong(address + REMOTE_COUNT_OFFSET);
        mp.pool.putLong(address + REMOTE_COUNT_OFFSET, current + remoteCount);
    }

    public static long getRemoteCpuTime(long address) {
        return getRemoteCpuTime(getMemoryPool(address),
                sanitizeAddress(address));
    }

    public static long getRemoteCpuTime(MemoryPool mp, int address) {
        return mp.pool.getLong(address + REMOTE_CPU_TIME_OFFSET);
    }

    public static void setRemoteCpuTime(long address, long remoteCpuTime) {
        setRemoteCpuTime(getMemoryPool(address), sanitizeAddress(address),
                remoteCpuTime);
    }

    public static void setRemoteCpuTime(MemoryPool mp, int address,
            long remoteCpuTime) {
        mp.pool.putLong(address + REMOTE_CPU_TIME_OFFSET, remoteCpuTime);
    }

    public static void incrementRemoteCpuTime(long address,
            long remoteCpuTime) {
        incrementRemoteCpuTime(getMemoryPool(address), sanitizeAddress(address),
                remoteCpuTime);
    }

    public static void incrementRemoteCpuTime(MemoryPool mp, int address,
            long remoteCpuTime) {
        long current = mp.pool.getLong(address + REMOTE_CPU_TIME_OFFSET);
        mp.pool.putLong(address + REMOTE_CPU_TIME_OFFSET,
                current + remoteCpuTime);
    }

    public static long getRemoteData(long address) {
        return getRemoteCount(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getRemoteData(MemoryPool mp, int address) {
        return mp.pool.getLong(address + REMOTE_DATA_OFFSET);
    }

    public static void setRemoteData(long address, long remoteData) {
        setRemoteData(getMemoryPool(address), sanitizeAddress(address),
                remoteData);
    }

    public static void setRemoteData(MemoryPool mp, int address,
            long remoteData) {
        mp.pool.putLong(address + REMOTE_DATA_OFFSET, remoteData);
    }

    public static void incrementRemoteData(long address, long remoteData) {
        incrementRemoteData(getMemoryPool(address), sanitizeAddress(address),
                remoteData);
    }

    public static void incrementRemoteData(MemoryPool mp, int address,
            long remoteData) {
        long current = mp.pool.getLong(address + REMOTE_DATA_OFFSET);
        mp.pool.putLong(address + REMOTE_DATA_OFFSET, current + remoteData);
    }

    @Override
    public void doAggregationImpl(MemoryPool mp, int address) {
        int aggregate = mp.pool.getInt(address + AGGREGATE_OFFSET);
        if (aggregate < 0) {
            return;
        }

        // see super for reasoning behind locking mechanism
        Object lock = LOCK_CACHE[(aggregate >> 1) & (LOCK_CACHE_SIZE - 1)];

        long startWait;
        if (Globals.LOCK_DIAGNOSTICS) {
            startWait = System.currentTimeMillis();
        }
        synchronized (lock) {
            if (Globals.LOCK_DIAGNOSTICS) {
                lockWaitTime.addAndGet(System.currentTimeMillis() - startWait);
            }

            incrementRemoteCount(mp, aggregate, getRemoteCount(mp, address));
            incrementRemoteCpuTime(mp, aggregate,
                    getRemoteCpuTime(mp, address));
            incrementRemoteData(mp, aggregate, getRemoteData(mp, address));
            super.doAggregationImpl(mp, address);
        }
    }

    @Override
    protected void getCsvHeadersImpl(MemoryPool mp, int address,
            String separator, StringBuilder sb) {
        super.getCsvHeadersImpl(mp, address, separator, sb);
        sb.append(separator).append("remoteCount");
        sb.append(separator).append("remoteCpuTime");
        sb.append(separator).append("remoteData");
    }

    @Override
    protected void toCsvImpl(MemoryPool mp, int address, String separator,
            StringBuilder sb) {
        super.toCsvImpl(mp, address, separator, sb);
        sb.append(separator).append(getRemoteCount(mp, address));
        sb.append(separator).append(getRemoteCpuTime(mp, address));
        sb.append(separator).append(getRemoteData(mp, address));
    }

    @Override
    protected void fromCsvImpl(String[] values, int off, MemoryPool mp,
            int address) {
        super.fromCsvImpl(values, off, mp, address);
        setRemoteCount(mp, address, Long.parseLong(values[off + 7]));
        setRemoteCpuTime(mp, address, Long.parseLong(values[off + 8]));
        setRemoteData(mp, address, Long.parseLong(values[off + 9]));
    }

    @Override
    protected void toFlatBufferImpl(MemoryPool mp, int address,
            FlatBufferBuilder builder) {
        super.toFlatBufferImpl(mp, address, builder);
        long remoteCount = getRemoteCount(mp, address);
        if (remoteCount > 0)
            OperationStatisticsFB.addRemoteCount(builder, remoteCount);
        long remoteCpuTime = getRemoteCpuTime(mp, address);
        if (remoteCpuTime > 0)
            OperationStatisticsFB.addRemoteCpuTime(builder, remoteCpuTime);
        long remoteData = getRemoteData(mp, address);
        if (remoteData > 0)
            OperationStatisticsFB.addRemoteData(builder, remoteData);
    }

    @Override
    protected void fromFlatBufferImpl(OperationStatisticsFB osfb, MemoryPool mp,
            int address) {
        super.fromFlatBufferImpl(osfb, mp, address);
        setRemoteCount(mp, address, osfb.remoteCount());
        setRemoteCpuTime(mp, address, osfb.remoteCpuTime());
        setRemoteData(mp, address, osfb.remoteData());
    }
}
