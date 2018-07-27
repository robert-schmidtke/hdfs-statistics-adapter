/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics.bb;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;
import de.zib.sfs.instrument.statistics.bb.ByteBufferUtil.NumberType;
import de.zib.sfs.instrument.util.MemoryPool;

public class OperationStatisticsBufferBuilder {

    private static final ThreadLocal<CharsetDecoder> DECODER = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charset.forName("US-ASCII").newDecoder();
        }
    };

    private static final ThreadLocal<CharBuffer> BUFFER = new ThreadLocal<CharBuffer>() {
        @Override
        protected CharBuffer initialValue() {
            return ByteBuffer.allocateDirect(512).asCharBuffer();
        }
    };

    public static void serialize(ByteBuffer hostnameBb, int pid,
            ByteBuffer keyBb, long address, ByteBuffer bb) {
        ByteBuffer hostname = hostnameBb.slice();
        ByteBuffer key = keyBb.slice();

        // 0-1: empty
        // 2-3: type (OS, DOS, RDOS)
        // 4: hasPid
        // 5-6: pidType
        // 7: hasCount
        // 8-9: countType
        // 10: hasCpuTime
        // 11-12: cpuTimeType
        // 13: hasFd
        // 14-15: fdType
        // 16: hasThreadId
        // 17-18: threadIdType
        // 19-31: empty
        int header = 0;

        // for the int header
        int size = 4;

        // 0 = OS, 1 = DOS, 2 = RDOS
        int ost = OperationStatistics.getOperationStatisticsOffset(address);
        header |= ost << 28;

        MemoryPool mp = OperationStatistics.getMemoryPool(address);
        int sanitizedAddress = OperationStatistics.sanitizeAddress(address);

        // an additional byte per extended header
        size += ost;

        NumberType ntPid = ByteBufferUtil.getNumberType(pid);
        if (pid != 0) {
            header |= 0b100 << 25;
            header |= ntPid.ordinal() << 25;
            size += ntPid.getSize();
        }

        long count = OperationStatistics.getCount(mp, sanitizedAddress);
        NumberType ntCount = ByteBufferUtil.getNumberType(count);
        if (count != 0) {
            header |= 0b100 << 22;
            header |= ntCount.ordinal() << 22;
            size += ntCount.getSize();
        }

        long cpuTime = OperationStatistics.getCpuTime(mp, sanitizedAddress);
        NumberType ntTime = ByteBufferUtil.getNumberType(cpuTime);
        if (cpuTime != 0) {
            header |= 0b100 << 19;
            header |= ntTime.ordinal() << 19;
            size += ntTime.getSize();
        }

        int fileDescriptor = OperationStatistics.getFileDescriptor(mp,
                sanitizedAddress);
        NumberType ntFd = ByteBufferUtil.getNumberType(fileDescriptor);
        if (fileDescriptor != 0) {
            header |= 0b100 << 16;
            header |= ntFd.ordinal() << 16;
            size += ntFd.getSize();
        }

        long threadId = OperationStatistics.getThreadId(mp, sanitizedAddress);
        NumberType ntThreadId = ByteBufferUtil.getNumberType(threadId);
        if (threadId != 0) {
            header |= 0b100 << 13;
            header |= ntThreadId.ordinal() << 13;
            size += ntThreadId.getSize();
        }

        byte[] extHeader = new byte[ost];

        NumberType ntData = null;
        if (ost >= OperationStatistics.DOS_OFFSET) {
            // see above for 0-31
            // 32: empty
            // 33: hasData
            // 34-35: dataType
            // 36-39: empty
            long data = DataOperationStatistics.getData(mp, sanitizedAddress);
            ntData = ByteBufferUtil.getNumberType(data);
            if (data != 0) {
                extHeader[0] |= 0b100 << 4;
                extHeader[0] |= ntData.ordinal() << 4;
                size += ntData.getSize();
            }
        }

        NumberType ntRemoteCount = null;
        NumberType ntRemoteCpuTime = null;
        NumberType ntRemoteData = null;
        if (ost >= OperationStatistics.RDOS_OFFSET) {
            // see above for 0-35
            // 36: hasRemoteCount
            // 37-38: remoteCountType
            // 39-40: empty
            // 41: hasRemoteCpuTime
            // 42-43: remoteCpuTimeType
            // 44: hasRemoteData
            // 45-46: remoteDataType
            // 47: empty
            long remoteCount = ReadDataOperationStatistics.getRemoteCount(mp,
                    sanitizedAddress);
            ntRemoteCount = ByteBufferUtil.getNumberType(remoteCount);
            if (remoteCount != 0) {
                extHeader[0] |= 0b100 << 1;
                extHeader[0] |= ntRemoteCount.ordinal() << 1;
                size += ntRemoteCount.getSize();
            }

            long remoteCpuTime = ReadDataOperationStatistics
                    .getRemoteCpuTime(mp, sanitizedAddress);
            ntRemoteCpuTime = ByteBufferUtil.getNumberType(remoteCpuTime);
            if (remoteCpuTime != 0) {
                extHeader[1] |= 0b100 << 4;
                extHeader[1] |= ntRemoteCpuTime.ordinal() << 4;
                size += ntRemoteCpuTime.getSize();
            }

            long remoteData = ReadDataOperationStatistics.getRemoteData(mp,
                    sanitizedAddress);
            ntRemoteData = ByteBufferUtil.getNumberType(remoteData);
            if (remoteData != 0) {
                extHeader[1] |= 0b100 << 1;
                extHeader[1] |= ntRemoteData.ordinal() << 1;
                size += ntRemoteData.getSize();
            }
        }

        int hostnameLength = hostname.remaining();
        if (hostnameLength - Byte.MAX_VALUE > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Length: " + hostnameLength);
        }
        size += hostnameLength;
        size += 1; // for encoding the length, 255 at most

        int keyLength = key.remaining();
        if (keyLength - Byte.MAX_VALUE > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("Length: " + keyLength);
        }
        size += keyLength;
        size += 1; // for encoding the length, 255 at most

        size += 8; // timeBin
        size += 1; // source
        size += 1; // category

        if (bb.remaining() < size) {
            throw new BufferOverflowException();
        }
        bb.order(ByteOrder.LITTLE_ENDIAN);

        // header
        bb.putInt(header);
        for (byte b : extHeader) {
            bb.put(b);
        }

        // mark for GC
        extHeader = null;

        // hostname
        bb.put((byte) (hostnameLength - Byte.MAX_VALUE)).put(hostname);

        // pid
        ntPid.putInt(bb, pid);

        // key
        bb.put((byte) (keyLength - Byte.MAX_VALUE)).put(key);

        // timeBin
        bb.putLong(OperationStatistics.getTimeBin(mp, sanitizedAddress));

        // count
        ntCount.putLong(bb, count);

        // cpuTime
        ntTime.putLong(bb, cpuTime);

        // source
        bb.put((byte) OperationStatistics.getSource(mp, sanitizedAddress)
                .ordinal());

        // category
        bb.put((byte) OperationStatistics.getCategory(mp, sanitizedAddress)
                .ordinal());

        // file descriptor
        ntFd.putInt(bb, fileDescriptor);

        // thread ID
        ntThreadId.putLong(bb, threadId);

        if (ost >= OperationStatistics.DOS_OFFSET) {
            ntData.putLong(bb,
                    DataOperationStatistics.getData(mp, sanitizedAddress));
        }

        if (ost >= OperationStatistics.RDOS_OFFSET) {
            ntRemoteCount.putLong(bb, ReadDataOperationStatistics
                    .getRemoteCount(mp, sanitizedAddress));
            ntRemoteCpuTime.putLong(bb, ReadDataOperationStatistics
                    .getRemoteCpuTime(mp, sanitizedAddress));
            ntRemoteData.putLong(bb, ReadDataOperationStatistics
                    .getRemoteData(mp, sanitizedAddress));
        }
    }

    public static void deserialize(ByteBuffer bb, long address) {
        bb.order(ByteOrder.LITTLE_ENDIAN);

        int header = bb.getInt();
        int ost = header >> 28;

        NumberType ntPid = NumberType.EMPTY;
        if ((header & (0b100 << 25)) > 0) {
            ntPid = NumberType.VALUES[(header & (0b011 << 25)) >> 25];
        }

        NumberType ntCount = NumberType.EMPTY;
        if ((header & (0b100 << 22)) > 0) {
            ntCount = NumberType.VALUES[(header & (0b011 << 22)) >> 22];
        }

        NumberType ntTime = NumberType.EMPTY;
        if ((header & (0b100 << 19)) > 0) {
            ntTime = NumberType.VALUES[(header & (0b011 << 19)) >> 19];
        }

        NumberType ntFd = NumberType.EMPTY;
        if ((header & (0b100 << 16)) > 0) {
            ntFd = NumberType.VALUES[(header & (0b011 << 16)) >> 16];
        }

        NumberType ntThreadId = NumberType.EMPTY;
        if ((header & (0b100 << 13)) > 0) {
            ntThreadId = NumberType.VALUES[(header & (0b011 << 13)) >> 13];
        }

        byte[] extHeader = new byte[ost];
        for (int i = 0; i < ost; ++i) {
            extHeader[i] = bb.get();
        }

        NumberType ntData = null;
        if (ost >= OperationStatistics.DOS_OFFSET) {
            if ((extHeader[0] & (0b100 << 4)) > 0) {
                ntData = NumberType.VALUES[(extHeader[0] & (0b011 << 4)) >> 4];
            } else {
                ntData = NumberType.EMPTY;
            }
        }

        NumberType ntRemoteCount = null;
        NumberType ntRemoteCpuTime = null;
        NumberType ntRemoteData = null;
        if (ost >= OperationStatistics.RDOS_OFFSET) {
            if ((extHeader[0] & (0b100 << 1)) > 0) {
                ntRemoteCount = NumberType.VALUES[(extHeader[0]
                        & (0b011 << 1)) >> 1];
            } else {
                ntRemoteCount = NumberType.EMPTY;
            }

            if ((extHeader[1] & (0b100 << 4)) > 0) {
                ntRemoteCpuTime = NumberType.VALUES[(extHeader[1]
                        & (0b011 << 4)) >> 4];
            } else {
                ntRemoteCpuTime = NumberType.EMPTY;
            }

            if ((extHeader[1] & (0b100 << 1)) > 0) {
                ntRemoteData = NumberType.VALUES[(extHeader[1]
                        & (0b011 << 1)) >> 1];
            } else {
                ntRemoteData = NumberType.EMPTY;
            }
        }

        CharsetDecoder decoder = DECODER.get();

        // hostname
        byte hostnameLength = (byte) (bb.get() + Byte.MAX_VALUE);

        if (bb.remaining() < hostnameLength) {
            throw new BufferUnderflowException();
        }
        CharBuffer cb = BUFFER.get();
        decoder.reset();
        ByteBuffer _bb = bb.slice();
        _bb.limit(hostnameLength);
        CoderResult cr = decoder.decode(_bb, cb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(e);
            }
        }
        String hostname = cb.flip().toString();
        cb.clear();
        bb.position(bb.position() + hostnameLength);

        // pid
        int pid = ntPid.getInt(bb);

        // key
        byte keyLength = (byte) (bb.get() + Byte.MAX_VALUE);

        if (bb.remaining() < keyLength) {
            throw new BufferUnderflowException();
        }
        decoder.reset();
        _bb = bb.slice();
        _bb.limit(keyLength);
        cr = decoder.decode(_bb, cb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(e);
            }
        }
        String key = cb.flip().toString();
        cb.clear();
        bb.position(bb.position() + keyLength);

        // timeBin
        long timeBin = bb.getLong();

        // count
        long count = ntCount.getLong(bb);

        // cpuTime
        long cpuTime = ntTime.getLong(bb);

        // source
        OperationSource source = OperationSource.VALUES[bb.get()];

        // category
        OperationCategory category = OperationCategory.VALUES[bb.get()];

        // file descriptor
        int fd = ntFd.getInt(bb);

        // thread ID
        long threadId = ntThreadId.getLong(bb);

        MemoryPool mp = OperationStatistics.getMemoryPool(address);
        int sanitizedAddress = OperationStatistics.sanitizeAddress(address);

        OperationStatistics.setTimeBin(mp, sanitizedAddress, timeBin);
        OperationStatistics.setCount(mp, sanitizedAddress, count);
        OperationStatistics.setCpuTime(mp, sanitizedAddress, cpuTime);
        OperationStatistics.setSource(mp, sanitizedAddress, source);
        OperationStatistics.setCategory(mp, sanitizedAddress, category);
        OperationStatistics.setFileDescriptor(mp, sanitizedAddress, fd);
        OperationStatistics.setThreadId(mp, sanitizedAddress, threadId);

        if (ost >= OperationStatistics.DOS_OFFSET) {
            DataOperationStatistics.setData(mp, sanitizedAddress,
                    ntData.getLong(bb));
        }

        if (ost >= OperationStatistics.RDOS_OFFSET) {
            ReadDataOperationStatistics.setRemoteCount(mp, sanitizedAddress,
                    ntRemoteCount.getLong(bb));
            ReadDataOperationStatistics.setRemoteCpuTime(mp, sanitizedAddress,
                    ntRemoteCpuTime.getLong(bb));
            ReadDataOperationStatistics.setRemoteData(mp, sanitizedAddress,
                    ntRemoteData.getLong(bb));
        }
    }

}
