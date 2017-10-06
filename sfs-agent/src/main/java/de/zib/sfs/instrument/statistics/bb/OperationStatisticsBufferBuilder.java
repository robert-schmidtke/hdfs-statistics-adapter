/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics.bb;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
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

    private final OperationStatistics os;
    private final ByteBuffer bb;

    private short header;
    private final byte[] headerExt;
    private int size;

    public final static ThreadLocal<CharsetEncoder> ENCODER = new ThreadLocal<CharsetEncoder>() {
        @Override
        protected CharsetEncoder initialValue() {
            return Charset.forName("US-ASCII").newEncoder();
        }
    };
    private final static ThreadLocal<CharsetDecoder> DECODER = new ThreadLocal<CharsetDecoder>() {
        @Override
        protected CharsetDecoder initialValue() {
            return Charset.forName("US-ASCII").newDecoder();
        }
    };

    private OperationStatisticsBufferBuilder(int headerExtLength,
            OperationStatistics os) {
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
        this.os = os;
        bb = null;
        headerExt = new byte[headerExtLength];
        size = headerExtLength + 2;
        setCountInfo(os.getCount());
        setCpuTimeInfo(os.getCpuTime());
        setFdInfo(os.getFileDescriptor());

        size += 8; // timeBin
        size += 1; // source
        size += 1; // category
    }

    public OperationStatisticsBufferBuilder(OperationStatistics os) {
        this(0, os);
        setOperationStatisticsType(OperationStatisticsType.OS);
    }

    private OperationStatisticsBufferBuilder(int headerExtLength,
            DataOperationStatistics dos) {
        // see above for 0-15
        // 16: empty
        // 17: hasData
        // 18-19: dataType
        // 20-23: empty
        this(headerExtLength, (OperationStatistics) dos);
        setDataInfo(dos.getData());
    }

    public OperationStatisticsBufferBuilder(DataOperationStatistics dos) {
        this(1, dos);
        setOperationStatisticsType(OperationStatisticsType.DOS);
    }

    private OperationStatisticsBufferBuilder(int headerExtLength,
            ReadDataOperationStatistics rdos) {
        // see above for 0-19
        // 20: hasRemoteCount
        // 21-22: remoteCountType
        // 23-24: empty
        // 25: hasRemoteCpuTime
        // 26-27: remoteCpuTimeType
        // 28: hasRemoteData
        // 29-30: remoteDataType
        // 31: empty
        this(headerExtLength, (DataOperationStatistics) rdos);
        setRemoteCountInfo(rdos.getRemoteCount());
        setRemoteCpuTimeInfo(rdos.getRemoteCpuTime());
        setRemoteDataInfo(rdos.getRemoteData());
    }

    public OperationStatisticsBufferBuilder(ReadDataOperationStatistics rdos) {
        this(2, rdos);
        setOperationStatisticsType(OperationStatisticsType.RDOS);
    }

    public OperationStatisticsBufferBuilder(ByteBuffer bb) {
        os = null;
        this.bb = bb;
        this.bb.order(ByteOrder.LITTLE_ENDIAN);

        header = this.bb.getShort();
        switch (getOperationStatisticsType()) {
        case OS:
            size = 2;
            break;
        case DOS:
            size = 3;
            break;
        case RDOS:
            size = 4;
            break;
        default:
            throw new IllegalArgumentException();
        }

        headerExt = new byte[size - 2];
        for (int i = 0; i < size - 2; ++i) {
            headerExt[i] = this.bb.get();
        }
    }

    public ByteBuffer serialize(String hostname, int pid, String key) {
        CharsetEncoder encoder = ENCODER.get();

        setPidInfo(pid);

        int hostnameLength = hostname.length();
        if (hostnameLength - Byte.MAX_VALUE > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(hostname);
        }
        size += hostnameLength;
        size += 1; // for encoding the length, 255 at most

        int keyLength = key.length();
        if (keyLength - Byte.MAX_VALUE > Byte.MAX_VALUE) {
            throw new IllegalArgumentException(key);
        }
        size += key.length();
        size += 1; // for encoding the length, 255 at most

        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.order(ByteOrder.LITTLE_ENDIAN);

        // header
        bb.putShort(header);
        for (byte b : headerExt) {
            bb.put(b);
        }

        // hostname
        bb.put((byte) (hostnameLength - Byte.MAX_VALUE));
        encoder.reset();
        CoderResult cr = encoder.encode(CharBuffer.wrap(hostname), bb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(hostname, e);
            }
        }

        // pid
        getPidInfo().putInt(bb, pid);

        // key
        bb.put((byte) (keyLength - Byte.MAX_VALUE));
        encoder.reset();
        cr = encoder.encode(CharBuffer.wrap(key), bb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(key, e);
            }
        }

        // timeBin
        bb.putLong(os.getTimeBin());

        // count
        getCountInfo().putLong(bb, os.getCount());

        // cpuTime
        getCpuTimeInfo().putLong(bb, os.getCpuTime());

        // source
        bb.put((byte) os.getSource().ordinal());

        // category
        bb.put((byte) os.getCategory().ordinal());

        // file descriptor
        getFdInfo().putInt(bb, os.getFileDescriptor());

        switch (getOperationStatisticsType()) {
        case OS:
            break;
        case DOS:
            // data
            getDataInfo().putLong(bb, ((DataOperationStatistics) os).getData());
            break;
        case RDOS:
            ReadDataOperationStatistics rdos = (ReadDataOperationStatistics) os;

            // data
            getDataInfo().putLong(bb, rdos.getData());

            // remoteCount
            getRemoteCountInfo().putLong(bb, rdos.getRemoteCount());

            // remoteCpuTime
            getRemoteCpuTimeInfo().putLong(bb, rdos.getRemoteCpuTime());

            // remoteData
            getRemoteDataInfo().putLong(bb, rdos.getRemoteData());
            break;
        default:
            throw new IllegalArgumentException();
        }

        bb.flip();
        return bb;
    }

    public OperationStatistics deserialize() {
        CharsetDecoder decoder = DECODER.get();

        // hostname
        byte hostnameLength = (byte) (bb.get() + Byte.MAX_VALUE);
        if (bb.remaining() < hostnameLength) {
            throw new BufferUnderflowException();
        }
        CharBuffer cb = CharBuffer.allocate(hostnameLength);
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
        bb.position(bb.position() + hostnameLength);

        // pid
        int pid = getPidInfo().getInt(bb);

        // key
        byte keyLength = (byte) (bb.get() + Byte.MAX_VALUE);
        if (bb.remaining() < keyLength) {
            throw new BufferUnderflowException();
        }
        cb = CharBuffer.allocate(keyLength);
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
        bb.position(bb.position() + keyLength);

        // timeBin
        long timeBin = bb.getLong();

        // count
        long count = getCountInfo().getLong(bb);

        // cpuTime
        long cpuTime = getCpuTimeInfo().getLong(bb);

        // source
        OperationSource source = OperationSource.values()[bb.get()];

        // category
        OperationCategory category = OperationCategory.values()[bb.get()];

        // file descriptor
        int fd = getFdInfo().getInt(bb);

        switch (getOperationStatisticsType()) {
        case OS:
            return new OperationStatistics(count, timeBin, cpuTime, source,
                    category, fd);
        case DOS:
            long dosData = getDataInfo().getLong(bb);
            return new DataOperationStatistics(count, timeBin, cpuTime, source,
                    category, fd, dosData);
        case RDOS:
            long rdosData = getDataInfo().getLong(bb);
            long remoteCount = getRemoteCountInfo().getLong(bb);
            long remoteCpuTime = getRemoteCpuTimeInfo().getLong(bb);
            long remoteData = getRemoteDataInfo().getLong(bb);
            return new ReadDataOperationStatistics(count, timeBin, cpuTime,
                    source, category, fd, rdosData, remoteCount, remoteCpuTime,
                    remoteData);
        default:
            throw new IllegalArgumentException();
        }
    }

    private void setOperationStatisticsType(OperationStatisticsType ost) {
        header &= ~0b0110000000000000;
        header |= ost.ordinal() << 12;
    }

    private OperationStatisticsType getOperationStatisticsType() {
        return OperationStatisticsType.values()[header >> 12];
    }

    private void setPidInfo(int pid) {
        setFieldInfo(9, pid);
    }

    private void setCountInfo(long count) {
        setFieldInfo(6, count);
    }

    private void setCpuTimeInfo(long cpuTime) {
        setFieldInfo(3, cpuTime);
    }

    private void setFdInfo(int fd) {
        setFieldInfo(0, fd);
    }

    private void setDataInfo(long data) {
        setFieldInfo(0, 4, data);
    }

    private void setRemoteCountInfo(long remoteCount) {
        setFieldInfo(0, 1, remoteCount);
    }

    private void setRemoteCpuTimeInfo(long remoteCpuTime) {
        setFieldInfo(1, 4, remoteCpuTime);
    }

    private void setRemoteDataInfo(long remoteData) {
        setFieldInfo(1, 1, remoteData);
    }

    private void setFieldInfo(int headerBitOffset, long fieldValue) {
        header &= ~(0b111 << headerBitOffset);
        if (fieldValue != 0) {
            header |= 0b100 << headerBitOffset;
            NumberType nt = ByteBufferUtil.getNumberType(fieldValue);
            header |= nt.ordinal() << headerBitOffset;
            size += nt.getSize();
        }
    }

    private void setFieldInfo(int headerOffset, int headerBitOffset,
            long fieldValue) {
        headerExt[headerOffset] &= ~(0b111 << headerBitOffset);
        if (fieldValue != 0) {
            headerExt[headerOffset] |= 0b100 << headerBitOffset;
            NumberType nt = ByteBufferUtil.getNumberType(fieldValue);
            headerExt[headerOffset] |= nt.ordinal() << headerBitOffset;
            size += nt.getSize();
        }
    }

    private NumberType getPidInfo() {
        return getFieldInfo(9);
    }

    private NumberType getCountInfo() {
        return getFieldInfo(6);
    }

    private NumberType getCpuTimeInfo() {
        return getFieldInfo(3);
    }

    private NumberType getFdInfo() {
        return getFieldInfo(0);
    }

    private NumberType getDataInfo() {
        return getFieldInfo(0, 4);
    }

    private NumberType getRemoteCountInfo() {
        return getFieldInfo(0, 1);
    }

    private NumberType getRemoteCpuTimeInfo() {
        return getFieldInfo(1, 4);
    }

    private NumberType getRemoteDataInfo() {
        return getFieldInfo(1, 1);
    }

    private NumberType getFieldInfo(int headerBitOffset) {
        // we have the header information, but the requested field is not set
        if (((header
                & (0b100 << headerBitOffset)) >> headerBitOffset) != 0b100) {
            return NumberType.EMPTY;
        }

        return NumberType.values()[(header
                & (0b11 << headerBitOffset)) >> headerBitOffset];
    }

    private NumberType getFieldInfo(int headerOffset, int headerBitOffset) {
        // extended header information we do not have
        if (headerOffset >= headerExt.length) {
            return NumberType.EMPTY;
        }

        // we have the header information, but the requested field is not set
        if (((headerExt[headerOffset]
                & (0b100 << headerBitOffset)) >> headerBitOffset) != 0b100) {
            return NumberType.EMPTY;
        }

        return NumberType.values()[(headerExt[headerOffset]
                & (0b11 << headerBitOffset)) >> headerBitOffset];
    }

}
