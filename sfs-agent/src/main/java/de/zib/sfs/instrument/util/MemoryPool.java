/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class MemoryPool {

    public static class OutOfMemoryException extends RuntimeException {
        private static final long serialVersionUID = 4987025963701460798L;
    }

    public static class IllegalAddressException extends RuntimeException {
        private static final long serialVersionUID = 7688859608105872482L;
    }

    private final int poolSize, chunkSize;

    public final ByteBuffer pool;

    private final int numAddresses;
    private final ByteBuffer addresses;

    private final AtomicInteger popIndex, pushIndex;
    private final long sanitizer;

    public MemoryPool(int poolSize, int chunkSize) {
        if (poolSize % chunkSize > 0) {
            throw new IllegalArgumentException(
                    "Pool size must be a multiple of chunk size.");
        }

        this.poolSize = poolSize;
        this.chunkSize = chunkSize;
        this.pool = ByteBuffer.allocateDirect(this.poolSize);
        this.numAddresses = this.poolSize / this.chunkSize;
        this.addresses = ByteBuffer.allocateDirect(this.numAddresses << 2);
        this.popIndex = new AtomicInteger(0);
        this.pushIndex = new AtomicInteger(this.numAddresses);

        // need this for handling overflow of the indices
        this.sanitizer = 2L * Integer.MAX_VALUE + 2L;

        for (int i = 0; i < this.numAddresses; ++i) {
            this.addresses.putInt(i << 2, i * this.chunkSize);
        }
    }

    public int alloc() throws OutOfMemoryException {
        int index = this.popIndex.getAndIncrement();
        int address;
        if (this.pushIndex.get() - index > 0) {
            address = this.addresses.getInt(sanitizeIndex(index) << 2);
        } else {
            throw new OutOfMemoryException();
        }

        return address;
    }

    public void free(int address) throws IllegalAddressException {
        int index = this.pushIndex.getAndIncrement();
        if (index - this.popIndex.get() >= this.numAddresses) {
            throw new IllegalAddressException();
        }

        this.addresses.putInt(sanitizeIndex(index) << 2, address);
    }

    public int remaining() {
        return this.pushIndex.get() - this.popIndex.get();
    }

    private int sanitizeIndex(int index) {
        if (index >= 0) {
            return index % this.numAddresses;
        }
        return (int) ((this.sanitizer + index) % this.numAddresses);
    }

    // methods for testing

    public void setPopIndex(int index) {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        this.popIndex.set(index);
    }

    public void setPushIndex(int index) {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        this.pushIndex.set(index);
    }

}
