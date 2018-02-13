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

public class LongQueue {

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

    private final ByteBuffer queue;

    private final int numElements;

    // pointers to the next long that can be polled/offered
    private final AtomicInteger pollIndex, offerIndex;
    private final long sanitizer;

    public LongQueue(int queueSize, File mmapDir) {
        this.numElements = queueSize;
        if (Integer.bitCount(this.numElements) != 1) {
            throw new IllegalArgumentException(
                    "Queue size is not a power of two.");
        }

        if (Integer.MAX_VALUE >> 3 < this.numElements) {
            throw new IllegalArgumentException(
                    "Cannot allocate more than " + (Integer.MAX_VALUE >> 3)
                            + " elements at once (" + this.numElements + ")");
        }

        AbstractSfsCallback.DISCARD_NEXT.set(Boolean.TRUE);
        if (mmapDir == null) {
            this.queue = ByteBuffer.allocateDirect(this.numElements << 3);
        } else {
            try {
                long id = Thread.currentThread().getId();
                long time = System.currentTimeMillis();

                File lqFile = new File(mmapDir,
                        "longqueue-" + id + "-" + time + ".lq");
                lqFile.deleteOnExit();
                RandomAccessFile lqRaf = new RandomAccessFile(lqFile, "rw");
                lqRaf.setLength(this.numElements << 3);
                this.queue = lqRaf.getChannel().map(MapMode.READ_WRITE, 0,
                        this.numElements << 3);
                lqRaf.getChannel().close();
                lqRaf.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        AbstractSfsCallback.DISCARD_NEXT.set(Boolean.FALSE);

        this.pollIndex = new AtomicInteger(0);
        this.offerIndex = new AtomicInteger(0);

        // need this for handling overflow of the indices
        this.sanitizer = 2L * Integer.MAX_VALUE + 2L;

        int lockCacheSize = 1024;
        String sizeString = System
                .getProperty("de.zib.sfs.longQueue.lockCacheSize");
        if (sizeString != null) {
            try {
                lockCacheSize = Integer.parseInt(sizeString);
            } catch (NumberFormatException e) {
                System.err.println(
                        "Invalid number for de.zib.sfs.longQueue.lockCacheSize: "
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

    public long poll() {
        loop: for (;;) {
            int index = this.pollIndex.get();
            if (this.offerIndex.get() - index > 0) {
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

                    if (this.pollIndex.compareAndSet(index, index + 1)) {
                        return this.queue.getLong(sanitizedIndex << 3);
                    } else {
                        // retry on failed cas
                    }
                }
            } else {
                // empty, no retry necessary
                break loop;
            }
        }

        return Long.MIN_VALUE;
    }

    public boolean offer(long value) {
        if (Globals.STRICT) {
            if (value == Long.MIN_VALUE) {
                throw new IllegalArgumentException("Long.MIN_VALUE ("
                        + Long.MIN_VALUE + ") is a reserved value.");
            }
        }

        for (;;) {
            int index = this.offerIndex.get();
            if (Globals.STRICT) {
                int pi = this.pollIndex.get();
                if (index - pi >= this.numElements) {
                    return false;
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
                maxLockWaitTime.updateAndGet((v) -> Math.max(v,
                        System.currentTimeMillis() - startWait));

                if (this.offerIndex.compareAndSet(index, index + 1)) {
                    this.queue.putLong(sanitizedIndex << 3, value);
                    return true;
                }
            }
        }
    }

    public int remaining() {
        return this.offerIndex.get() - this.pollIndex.get();
    }

    public int capacity() {
        return this.numElements;
    }

    private int sanitizeIndex(int index) {
        if (index >= 0) {
            return index & (this.numElements - 1);
        }
        return (int) ((this.sanitizer + index) & (this.numElements - 1));
    }

    // methods for testing

    public void setPollIndex(int index) {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        this.pollIndex.set(index);
    }

    public void setOfferIndex(int index) {
        boolean callMe = false;
        assert (callMe = true);
        if (!callMe) {
            throw new Error("Only to be called when assertions are enabled.");
        }

        this.offerIndex.set(index);
    }

}
