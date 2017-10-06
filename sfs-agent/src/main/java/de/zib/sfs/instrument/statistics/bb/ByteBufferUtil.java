/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics.bb;

import java.nio.ByteBuffer;

public class ByteBufferUtil {

    public static interface ByteBufferIO {
        public void putInt(ByteBuffer bb, int value);

        public void putLong(ByteBuffer bb, long value);

        public int getInt(ByteBuffer bb);

        public long getLong(ByteBuffer bb);
    }

    public static enum NumberType implements ByteBufferIO {
        BYTE((byte) 1, new ByteBufferIO() {
            @Override
            public void putInt(ByteBuffer bb, int value) {
                bb.put((byte) value);
            }

            @Override
            public void putLong(ByteBuffer bb, long value) {
                bb.put((byte) value);
            }

            @Override
            public int getInt(ByteBuffer bb) {
                return bb.get();
            }

            @Override
            public long getLong(ByteBuffer bb) {
                return bb.get();
            }
        }), SHORT((byte) 2, new ByteBufferIO() {
            @Override
            public void putInt(ByteBuffer bb, int value) {
                bb.putShort((short) value);
            }

            @Override
            public void putLong(ByteBuffer bb, long value) {
                bb.putShort((short) value);
            }

            @Override
            public int getInt(ByteBuffer bb) {
                return bb.getShort();
            }

            @Override
            public long getLong(ByteBuffer bb) {
                return bb.getShort();
            }
        }), INT((byte) 4, new ByteBufferIO() {
            @Override
            public void putInt(ByteBuffer bb, int value) {
                bb.putInt(value);
            }

            @Override
            public void putLong(ByteBuffer bb, long value) {
                bb.putInt((int) value);
            }

            @Override
            public int getInt(ByteBuffer bb) {
                return bb.getInt();
            }

            @Override
            public long getLong(ByteBuffer bb) {
                return bb.getInt();
            }
        }), LONG((byte) 8, new ByteBufferIO() {
            @Override
            public void putInt(ByteBuffer bb, int value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void putLong(ByteBuffer bb, long value) {
                bb.putLong(value);
            }

            @Override
            public int getInt(ByteBuffer bb) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getLong(ByteBuffer bb) {
                return bb.getLong();
            }
        }), EMPTY((byte) 0, new ByteBufferIO() {
            @Override
            public void putInt(ByteBuffer bb, int value) {
            }

            @Override
            public void putLong(ByteBuffer bb, long value) {
            }

            @Override
            public int getInt(ByteBuffer bb) {
                return 0;
            }

            @Override
            public long getLong(ByteBuffer bb) {
                return 0;
            }
        });

        private final byte size;
        private final ByteBufferIO bbio;

        NumberType(byte size, ByteBufferIO bbio) {
            this.size = size;
            this.bbio = bbio;
        }

        public byte getSize() {
            return size;
        }

        public void putInt(ByteBuffer bb, int value) {
            bbio.putInt(bb, value);
        }

        public void putLong(ByteBuffer bb, long value) {
            bbio.putLong(bb, value);
        }

        public int getInt(ByteBuffer bb) {
            return bbio.getInt(bb);
        }

        public long getLong(ByteBuffer bb) {
            return bbio.getLong(bb);
        }
    }

    public static NumberType getNumberType(long l) {
        if (l == 0) {
            return NumberType.EMPTY;
        } else if (Byte.MIN_VALUE <= l && l <= Byte.MAX_VALUE) {
            return NumberType.BYTE;
        } else if (Short.MIN_VALUE <= l && l <= Short.MAX_VALUE) {
            return NumberType.SHORT;
        } else if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE) {
            return NumberType.INT;
        } else {
            return NumberType.LONG;
        }
    }

}
