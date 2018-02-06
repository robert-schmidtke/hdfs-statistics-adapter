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
import java.util.concurrent.atomic.AtomicLong;

public class LongQueue {

    public static class OutOfMemoryException extends RuntimeException {
        private static final long serialVersionUID = 4987025963701460798L;

        public OutOfMemoryException(String message) {
            super(message);
        }
    }

    protected final Object[] lockCache;
    protected final int lockCacheSize;
    public static final AtomicLong lockWaitTime;
    static {
        if (Globals.LOCK_DIAGNOSTICS) {
            lockWaitTime = new AtomicLong(0);
        } else {
            lockWaitTime = null;
        }
    }

    private final ByteBuffer queue;

    private final int numElements;

    // pointers to the next long that can be polled/offered
    private final AtomicInteger pollIndex, offerIndex;
    private final long sanitizer;

    public LongQueue(int queueSize) {
        this.numElements = queueSize;
        if (Integer.bitCount(this.numElements) != 1) {
            throw new IllegalArgumentException(
                    "Queue size is not a power of two.");
        }

        this.queue = ByteBuffer.allocateDirect(this.numElements << 3);
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
                        lockWaitTime.addAndGet(
                                System.currentTimeMillis() - startWait);
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

    public void offer(long value) {
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
                    throw new OutOfMemoryException(
                            index + ", " + pi + ", " + this.numElements);
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
                    lockWaitTime
                            .addAndGet(System.currentTimeMillis() - startWait);
                }

                if (this.offerIndex.compareAndSet(index, index + 1)) {
                    this.queue.putLong(sanitizedIndex << 3, value);
                    return;
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
