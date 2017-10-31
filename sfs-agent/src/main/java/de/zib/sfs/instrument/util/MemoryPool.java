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

    public final ByteBuffer pool;

    private final int numAddresses;
    private final ByteBuffer addresses;

    // pointers to the next address that can be allocated/freed
    protected AtomicInteger allocIndex, freeIndex;
    private final long sanitizer;

    public MemoryPool(int poolSize, int chunkSize) {
        if (poolSize % chunkSize > 0) {
            throw new IllegalArgumentException(
                    "Pool size must be a multiple of chunk size.");
        }

        this.pool = ByteBuffer.allocateDirect(poolSize);
        this.numAddresses = poolSize / chunkSize;
        this.addresses = ByteBuffer.allocateDirect(this.numAddresses << 2);
        this.allocIndex = new AtomicInteger(0);
        this.freeIndex = new AtomicInteger(this.numAddresses);

        // need this for handling overflow of the indices
        this.sanitizer = 2L * Integer.MAX_VALUE + 2L;

        for (int i = 0; i < this.numAddresses; ++i) {
            this.addresses.putInt(i << 2, i * chunkSize);
        }
    }

    public int alloc() throws OutOfMemoryException {
        int index = this.allocIndex.getAndIncrement();
        int address;
        if (this.freeIndex.get() - index > 0) {
            address = this.addresses.getInt(sanitizeIndex(index) << 2);
        } else {
            throw new OutOfMemoryException();
        }

        return address;
    }

    public void free(int address) throws IllegalAddressException {
        int index = this.freeIndex.getAndIncrement();
        if (index - this.allocIndex.get() >= this.numAddresses) {
            throw new IllegalAddressException();
        }

        this.addresses.putInt(sanitizeIndex(index) << 2, address);
    }

    public int remaining() {
        return this.freeIndex.get() - this.allocIndex.get();
    }

    private int sanitizeIndex(int index) {
        if (index >= 0) {
            return index % this.numAddresses;
        }
        return (int) ((this.sanitizer + index) % this.numAddresses);
    }

    // methods for testing

    public void setAllocIndex(int index) {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        this.allocIndex.set(index);
    }

    public void setFreeIndex(int index) {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        this.freeIndex.set(index);
    }

}
