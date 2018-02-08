/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import de.zib.sfs.instrument.AbstractSfsCallback;

public class MemoryPool {

    public final ByteBuffer pool;

    private final int numAddresses;
    private final ByteBuffer addresses;

    // pointers to the next address that can be allocated/freed
    protected AtomicInteger allocIndex, freeIndex;
    private final long sanitizer;

    protected final Object[] lockCache;
    protected final int lockCacheSize;
    public static final AtomicLong maxLockWaitTime;
    static {
        if (Globals.LOCK_DIAGNOSTICS) {
            maxLockWaitTime = new AtomicLong(0);
        } else {
            maxLockWaitTime = null;
        }
    }

    public MemoryPool(int poolSize, int chunkSize/*, boolean mmap*/) {
        if (poolSize % chunkSize > 0) {
            throw new IllegalArgumentException(
                    "Pool size must be a multiple of chunk size.");
        }

        this.numAddresses = poolSize / chunkSize;
        if (Integer.bitCount(this.numAddresses) != 1) {
            throw new IllegalArgumentException(
                    "Number of elements is not a power of two.");
        }

        if (Integer.MAX_VALUE >> 2 < this.numAddresses) {
            throw new IllegalArgumentException("Cannot allocate more than "
                    + (Integer.MAX_VALUE >> 2) + " chunks of size " + chunkSize
                    + " at once (" + this.numAddresses + ")");
        }

        boolean mmap = true;

        // do not add instrumentation to the instances created in the next
        // clauses
        AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
        if (!mmap) {
            this.pool = ByteBuffer.allocateDirect(poolSize);
            this.addresses = ByteBuffer.allocateDirect(this.numAddresses << 2);
        } else {
            try {
                long id = Thread.currentThread().getId();
                long time = System.currentTimeMillis();

                // File$TempDirectory may not be available at this point, so
                // roll our own
                File tmpDir = new File(System.getProperty("java.io.tmpdir"));

                File mpFile = new File(tmpDir,
                        "memorypool-" + id + "-" + time + ".mp");
                mpFile.deleteOnExit();
                RandomAccessFile mpRaf = new RandomAccessFile(mpFile, "rw");
                mpRaf.setLength(poolSize);
                this.pool = mpRaf.getChannel().map(MapMode.READ_WRITE, 0,
                        poolSize);
                mpRaf.getChannel().close();
                mpRaf.close();

                File addrFile = new File(tmpDir,
                        "addresses-" + id + "-" + time + ".mp");
                addrFile.deleteOnExit();
                RandomAccessFile addrRaf = new RandomAccessFile(addrFile, "rw");
                addrRaf.setLength(this.numAddresses << 2);
                this.addresses = addrRaf.getChannel().map(MapMode.READ_WRITE, 0,
                        this.numAddresses << 2);
                addrRaf.getChannel().close();
                addrRaf.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);

        this.allocIndex = new AtomicInteger(0);
        this.freeIndex = new AtomicInteger(this.numAddresses);

        // need this for handling overflow of the indices
        this.sanitizer = 2L * Integer.MAX_VALUE + 2L;

        for (int i = 0; i < this.numAddresses; ++i) {
            this.addresses.putInt(i << 2, i * chunkSize);
        }

        int lockCacheSize = 1024;
        String sizeString = System
                .getProperty("de.zib.sfs.memoryPool.lockCacheSize");
        if (sizeString != null) {
            try {
                lockCacheSize = Integer.parseInt(sizeString);
            } catch (NumberFormatException e) {
                System.err.println(
                        "Invalid number for de.zib.sfs.memoryPool.lockCacheSize: "
                                + sizeString + ", falling back to "
                                + lockCacheSize + ".");
            }

            if (Integer.bitCount(lockCacheSize) != 1) {
                throw new IllegalArgumentException(
                        "Lock cache size is not a power of two.");
            }
        }
        this.lockCache = new Object[this.lockCacheSize = lockCacheSize];
        for (int i = 0; i < this.lockCacheSize; ++i) {
            this.lockCache[i] = new Object();
        }
    }

    public int alloc() {
        for (;;) {
            int index = this.allocIndex.get();
            if (this.freeIndex.get() - index <= 0) {
                return -1;
            }

            int sanitizedIndex = sanitizeIndex(index);
            Object lock = this.lockCache[sanitizedIndex
                    & (this.lockCacheSize - 1)];

            long startWait;
            if (Globals.LOCK_DIAGNOSTICS) {
                startWait = System.currentTimeMillis();
            }
            synchronized (lock) {
                if (Globals.LOCK_DIAGNOSTICS) {
                    maxLockWaitTime.updateAndGet((v) -> Math.max(v,
                            System.currentTimeMillis() - startWait));
                }

                if (this.allocIndex.compareAndSet(index, index + 1)) {
                    return this.addresses.getInt(sanitizedIndex << 2);
                }
            }
        }
    }

    public void free(int address) {
        for (;;) {
            int index = this.freeIndex.get();
            if (Globals.STRICT) {
                if (index - this.allocIndex.get() >= this.numAddresses) {
                    throw new IllegalArgumentException(
                            "Trying to free too many addresses: " + address);
                }
            }

            int sanitizedIndex = sanitizeIndex(index);
            Object lock = this.lockCache[sanitizedIndex
                    & (this.lockCacheSize - 1)];

            long startWait;
            if (Globals.LOCK_DIAGNOSTICS) {
                startWait = System.currentTimeMillis();
            }
            synchronized (lock) {
                if (Globals.LOCK_DIAGNOSTICS) {
                    maxLockWaitTime.updateAndGet((v) -> Math.max(v,
                            System.currentTimeMillis() - startWait));
                }

                if (this.freeIndex.compareAndSet(index, index + 1)) {
                    this.addresses.putInt(sanitizedIndex << 2, address);
                    return;
                }
            }
        }
    }

    public int remaining() {
        return this.freeIndex.get() - this.allocIndex.get();
    }

    private int sanitizeIndex(int index) {
        if (index >= 0) {
            return index & (this.numAddresses - 1);
        }
        return (int) ((this.sanitizer + index) & (this.numAddresses - 1));
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
