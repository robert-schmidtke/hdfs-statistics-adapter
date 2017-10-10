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
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

import de.zib.sfs.instrument.statistics.FileDescriptorMapping;
import de.zib.sfs.instrument.statistics.bb.ByteBufferUtil.NumberType;

public class FileDescriptorMappingBufferBuilder {

    private static final ThreadLocal<CharsetEncoder> ENCODER = new ThreadLocal<CharsetEncoder>() {
        @Override
        protected CharsetEncoder initialValue() {
            return Charset.forName("US-ASCII").newEncoder();
        }
    };

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

    public static void serialize(int fileDescriptor, String path,
            ByteBuffer hostnameBb, int pid, ByteBuffer keyBb, ByteBuffer bb) {
        ByteBuffer hostname = hostnameBb.slice();
        ByteBuffer key = keyBb.slice();

        // header byte:
        // 0-1: empty
        // 2-3: pidType
        // 4-5: fdType
        // 6-7: lengthType
        byte header = 0;

        int length = path.length();
        NumberType ntPid = ByteBufferUtil.getNumberType(pid);
        NumberType ntFd = ByteBufferUtil.getNumberType(fileDescriptor);
        NumberType ntLength = ByteBufferUtil.getNumberType(length);

        int size = ntPid.getSize();
        size += ntFd.getSize();
        size += ntLength.getSize();

        size += hostname.remaining();
        size += 1;

        size += key.remaining();
        size += 1;

        if (bb.remaining() < size) {
            throw new BufferOverflowException();
        }
        bb.order(ByteOrder.LITTLE_ENDIAN);

        header |= (ntPid.ordinal() << 4) | (ntFd.ordinal() << 2)
                | ntLength.ordinal();
        bb.put(header);

        CharsetEncoder encoder = ENCODER.get();

        // hostname
        bb.put((byte) (hostname.remaining() - Byte.MAX_VALUE)).put(hostname);

        // pid
        ntPid.putInt(bb, pid);

        // key
        bb.put((byte) (key.remaining() - Byte.MAX_VALUE)).put(key);

        // file descriptor
        ntFd.putInt(bb, fileDescriptor);

        // path
        ntLength.putInt(bb, length);
        if (length > bb.remaining()) {
            throw new BufferOverflowException();
        }
        encoder.reset();

        CharBuffer cb = BUFFER.get();
        int capacity = cb.capacity();
        if (length > capacity) {
            cb = CharBuffer.allocate(capacity << 1);
            BUFFER.set(cb);
        }
        cb.put(path);
        cb.flip();

        CoderResult cr = encoder.encode(cb, bb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(path, e);
            }
        }
        cb.clear();
    }

    public static void deserialize(ByteBuffer bb, FileDescriptorMapping fdm) {
        bb.order(ByteOrder.LITTLE_ENDIAN);

        byte header = bb.get();
        NumberType ntPid = NumberType.values()[(header & 0b00110000) >> 4];
        NumberType ntFd = NumberType.values()[(header & 0b00001100) >> 2];
        NumberType ntLength = NumberType.values()[header & 0b00000011];

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
        int pid = ntPid.getInt(bb);

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

        // file descriptor
        int fileDescriptor = ntFd.getInt(bb);

        // path
        int pathLength = ntLength.getInt(bb);
        if (bb.remaining() < pathLength) {
            throw new BufferUnderflowException();
        }
        cb = CharBuffer.allocate(pathLength);
        decoder.reset();
        _bb = bb.slice();
        _bb.limit(pathLength);
        cr = decoder.decode(_bb, cb, true);
        if (cr.isError()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IllegalArgumentException(e);
            }
        }
        String path = cb.flip().toString();
        bb.position(bb.position() + pathLength);

        fdm.setHostname(hostname);
        fdm.setPid(pid);
        fdm.setKey(key);
        fdm.setFd(fileDescriptor);
        fdm.setPath(path);
    }

}
