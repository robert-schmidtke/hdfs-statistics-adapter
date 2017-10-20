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

public class OperationStatisticsBufferBuilder {

    public static enum OperationStatisticsType {
        OS, DOS, RDOS;
    }

    private static final ThreadLocal<CharsetDecoder> DECODER = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charset.forName("US-ASCII").newDecoder();
        }
    };

    private static final ThreadLocal<CharBuffer> BUFFER = new ThreadLocal<CharBuffer>() {
        @Override
        protected CharBuffer initialValue() {
            return CharBuffer.allocate(256);
        }
    };

    public static void serialize(ByteBuffer hostnameBb, int pid,
            ByteBuffer keyBb, OperationStatistics os, ByteBuffer bb) {
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
        short header = 0;

        // for the short header
        int size = 2;

        int ost = OperationStatisticsType.OS.ordinal();
        if (os instanceof ReadDataOperationStatistics) {
            ost = OperationStatisticsType.RDOS.ordinal();
        } else if (os instanceof DataOperationStatistics) {
            ost = OperationStatisticsType.DOS.ordinal();
        }
        header |= ost << 12;

        // an additional byte per extended header
        size += ost;

        NumberType ntPid = ByteBufferUtil.getNumberType(pid);
        if (pid != 0) {
            header |= 0b100 << 9;
            header |= ntPid.ordinal() << 9;
            size += ntPid.getSize();
        }

        NumberType ntCount = ByteBufferUtil.getNumberType(os.getCount());
        if (os.getCount() != 0) {
            header |= 0b100 << 6;
            header |= ntCount.ordinal() << 6;
            size += ntCount.getSize();
        }

        NumberType ntTime = ByteBufferUtil.getNumberType(os.getCpuTime());
        if (os.getCpuTime() != 0) {
            header |= 0b100 << 3;
            header |= ntTime.ordinal() << 3;
            size += ntTime.getSize();
        }

        NumberType ntFd = ByteBufferUtil.getNumberType(os.getFileDescriptor());
        if (os.getFileDescriptor() != 0) {
            header |= 0b100;
            header |= ntFd.ordinal();
            size += ntFd.getSize();
        }

        byte[] extHeader = new byte[ost];

        NumberType ntData = null;
        if (ost > 0) {
            // see above for 0-15
            // 16: empty
            // 17: hasData
            // 18-19: dataType
            // 20-23: empty
            DataOperationStatistics dos = (DataOperationStatistics) os;
            ntData = ByteBufferUtil.getNumberType(dos.getData());
            if (dos.getData() != 0) {
                extHeader[0] |= 0b100 << 4;
                extHeader[0] |= ntData.ordinal() << 4;
                size += ntData.getSize();
            }
        }

        NumberType ntRemoteCount = null;
        NumberType ntRemoteTime = null;
        NumberType ntRemoteData = null;
        if (ost > 1) {
            // see above for 0-19
            // 20: hasRemoteCount
            // 21-22: remoteCountType
            // 23-24: empty
            // 25: hasRemoteCpuTime
            // 26-27: remoteCpuTimeType
            // 28: hasRemoteData
            // 29-30: remoteDataType
            // 31: empty
            ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) os;

            ntRemoteCount = ByteBufferUtil.getNumberType(rdos.getRemoteCount());
            if (rdos.getRemoteCount() != 0) {
                extHeader[0] |= 0b100 << 1;
                extHeader[0] |= ntRemoteCount.ordinal() << 1;
                size += ntRemoteCount.getSize();
            }

            ntRemoteTime = ByteBufferUtil
                    .getNumberType(rdos.getRemoteCpuTime());
            if (rdos.getRemoteCpuTime() != 0) {
                extHeader[1] |= 0b100 << 4;
                extHeader[1] |= ntRemoteTime.ordinal() << 4;
                size += ntRemoteTime.getSize();
            }

            ntRemoteData = ByteBufferUtil.getNumberType(rdos.getRemoteData());
            if (rdos.getRemoteData() != 0) {
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
        bb.putShort(header);
        for (byte b : extHeader) {
            bb.put(b);
        }

        // hostname
        bb.put((byte) (hostnameLength - Byte.MAX_VALUE)).put(hostname);

        // pid
        ntPid.putInt(bb, pid);

        // key
        bb.put((byte) (keyLength - Byte.MAX_VALUE)).put(key);

        // timeBin
        bb.putLong(os.getTimeBin());

        // count
        ntCount.putLong(bb, os.getCount());

        // cpuTime
        ntTime.putLong(bb, os.getCpuTime());

        // source
        bb.put((byte) os.getSource().ordinal());

        // category
        bb.put((byte) os.getCategory().ordinal());

        // file descriptor
        ntFd.putInt(bb, os.getFileDescriptor());

        if (ost > 0) {
            DataOperationStatistics dos = (DataOperationStatistics) os;
            ntData.putLong(bb, dos.getData());
        }

        if (ost > 1) {
            ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) os;
            ntRemoteCount.putLong(bb, rdos.getRemoteCount());
            ntRemoteTime.putLong(bb, rdos.getRemoteCpuTime());
            ntRemoteData.putLong(bb, rdos.getRemoteData());
        }
    }

    public static void deserialize(ByteBuffer bb, OperationStatistics os) {
        bb.order(ByteOrder.LITTLE_ENDIAN);

        short header = bb.getShort();
        int ost = header >> 12;

        NumberType ntPid = NumberType.EMPTY;
        if ((header & (0b100 << 9)) > 0) {
            ntPid = NumberType.VALUES[(header & (0b011 << 9)) >> 9];
        }

        NumberType ntCount = NumberType.EMPTY;
        if ((header & (0b100 << 6)) > 0) {
            ntCount = NumberType.VALUES[(header & (0b011 << 6)) >> 6];
        }

        NumberType ntTime = NumberType.EMPTY;
        if ((header & (0b100 << 3)) > 0) {
            ntTime = NumberType.VALUES[(header & (0b011 << 3)) >> 3];
        }

        NumberType ntFd = NumberType.EMPTY;
        if ((header & 0b100) > 0) {
            ntFd = NumberType.VALUES[header & 0b011];
        }

        byte[] extHeader = new byte[ost];
        for (int i = 0; i < ost; ++i) {
            extHeader[i] = bb.get();
        }

        NumberType ntData = null;
        if (ost > 0) {
            if ((extHeader[0] & (0b100 << 4)) > 0) {
                ntData = NumberType.VALUES[(extHeader[0] & (0b011 << 4)) >> 4];
            } else {
                ntData = NumberType.EMPTY;
            }
        }

        NumberType ntRemoteCount = null;
        NumberType ntRemoteTime = null;
        NumberType ntRemoteData = null;
        if (ost > 1) {
            if ((extHeader[0] & (0b100 << 1)) > 0) {
                ntRemoteCount = NumberType.VALUES[(extHeader[0]
                        & (0b011 << 1)) >> 1];
            } else {
                ntRemoteCount = NumberType.EMPTY;
            }

            if ((extHeader[1] & (0b100 << 4)) > 0) {
                ntRemoteTime = NumberType.VALUES[(extHeader[1]
                        & (0b011 << 4)) >> 4];
            } else {
                ntRemoteTime = NumberType.EMPTY;
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

        os.setTimeBin(timeBin);
        os.setCount(count);
        os.setCpuTime(cpuTime);
        os.setSource(source);
        os.setCategory(category);
        os.setFileDescriptor(fd);

        if (ost > 0) {
            DataOperationStatistics dos = (DataOperationStatistics) os;
            dos.setData(ntData.getLong(bb));
        }

        if (ost > 1) {
            ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) os;
            rdos.setRemoteCount(ntRemoteCount.getLong(bb));
            rdos.setRemoteCpuTime(ntRemoteTime.getLong(bb));
            rdos.setRemoteData(ntRemoteData.getLong(bb));
        }
    }

}
