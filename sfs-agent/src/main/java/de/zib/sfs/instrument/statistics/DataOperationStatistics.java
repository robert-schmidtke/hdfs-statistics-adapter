/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import com.google.flatbuffers.FlatBufferBuilder;

import de.zib.sfs.instrument.statistics.fb.OperationStatisticsFB;
import de.zib.sfs.instrument.util.MemoryPool;

public class DataOperationStatistics extends OperationStatistics {

    private static final int DATA_OFFSET = OperationStatistics.SIZE; // long
    protected static final int SIZE = DATA_OFFSET + 8;

    private static final int MAX_POOL_SIZE;
    static {
        // see super
        int maxBytes = 536870911;
        MAX_POOL_SIZE = (maxBytes - (maxBytes % SIZE) - SIZE) / SIZE;
    }

    public static int getDataOperationStatistics() {
        if (memory[DOS_OFFSET] == null) {
            synchronized (DataOperationStatistics.class) {
                if (memory[DOS_OFFSET] == null) {
                    memory[DOS_OFFSET] = new MemoryPool(SIZE * MAX_POOL_SIZE,
                            SIZE);
                    impl[DOS_OFFSET] = new DataOperationStatistics();
                }
            }
        }

        int address = memory[DOS_OFFSET].alloc();
        return address | (DOS_OFFSET << 29);
    }

    public static int getDataOperationStatistics(long count, long timeBin,
            long cpuTime, OperationSource source, OperationCategory category,
            int fd, long data) {
        int address = getDataOperationStatistics();
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

    public static int getDataOperationStatistics(long timeBinDuration,
            OperationSource source, OperationCategory category, long startTime,
            long endTime, int fd, long data) {
        return getDataOperationStatistics(1,
                startTime - startTime % timeBinDuration, endTime - startTime,
                source, category, fd, data);
    }

    public static long getData(int address) {
        return getData(getMemoryPool(address), sanitizeAddress(address));
    }

    public static long getData(MemoryPool mp, int address) {
        return mp.pool.getLong(address + DATA_OFFSET);
    }

    public static void setData(int address, long data) {
        setData(getMemoryPool(address), sanitizeAddress(address), data);
    }

    public static void setData(MemoryPool mp, int address, long data) {
        mp.pool.putLong(address + DATA_OFFSET, data);
    }

    public static void incrementData(int address, long data) {
        incrementData(getMemoryPool(address), sanitizeAddress(address), data);
    }

    public static void incrementData(MemoryPool mp, int address, long data) {
        long current = mp.pool.getLong(address + DATA_OFFSET);
        mp.pool.putLong(address + DATA_OFFSET, current + data);
    }

    @Override
    protected void doAggregationImpl(MemoryPool mp, int address) {
        int aggregate = mp.pool.getInt(address + AGGREGATE_OFFSET);
        if (aggregate < 0) {
            return;
        }

        // see super for reasoning behind locking mechanism
        Object lock = LOCK_CACHE[(aggregate >> 1) % LOCK_CACHE_SIZE];
        synchronized (lock) {
            incrementData(mp, aggregate, getData(mp, address));
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
}
