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

public class IntQueue {

    public static class OutOfMemoryException extends RuntimeException {
        private static final long serialVersionUID = 4987025963701460798L;
    }

    protected static final Object[] LOCK_CACHE;
    protected static final int LOCK_CACHE_SIZE;
    public static final AtomicLong lockWaitTime;
    static {
        int size = 1024;
        String sizeString = System.getProperty("de.zib.sfs.lockCache.iq.size");
        if (sizeString != null) {
            try {
                size = Integer.parseInt(sizeString);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        LOCK_CACHE = new Object[LOCK_CACHE_SIZE = size];
        for (int i = 0; i < LOCK_CACHE_SIZE; ++i) {
            LOCK_CACHE[i] = new Object();
        }

        if (Globals.LOCK_DIAGNOSTICS) {
            lockWaitTime = new AtomicLong(0);
        } else {
            lockWaitTime = null;
        }
    }

    private final ByteBuffer queue;

    private final int numElements;

    // pointers to the next int that can be polled/offered
    private final AtomicInteger pollIndex, offerIndex;
    private final long sanitizer;

    public IntQueue(int queueSize) {
        this.queue = ByteBuffer.allocateDirect(queueSize << 2);
        this.numElements = queueSize;
        this.pollIndex = new AtomicInteger(0);
        this.offerIndex = new AtomicInteger(0);

        // need this for handling overflow of the indices
        this.sanitizer = 2L * Integer.MAX_VALUE + 2L;
    }

    public int poll() {
        int index = this.pollIndex.get();
        if (this.offerIndex.get() - index > 0) {
            int sanitizedIndex = sanitizeIndex(index);

            Object lock = LOCK_CACHE[sanitizedIndex % LOCK_CACHE_SIZE];
            long startWait;
            if (Globals.LOCK_DIAGNOSTICS) {
                startWait = System.currentTimeMillis();
            }
            synchronized (lock) {
                if (Globals.LOCK_DIAGNOSTICS) {
                    lockWaitTime
                            .addAndGet(System.currentTimeMillis() - startWait);
                }

                if (this.pollIndex.compareAndSet(index, index + 1)) {
                    return this.queue.getInt(sanitizedIndex << 2);
                }
            }
        }
        return Integer.MIN_VALUE;
    }

    public void offer(int value) {
        if (Globals.STRICT) {
            if (value == Integer.MIN_VALUE) {
                throw new IllegalArgumentException("Integer.MIN_VALUE ("
                        + Integer.MIN_VALUE + ") is a reserved value.");
            }
        }

        for (;;) {
            int index = this.offerIndex.get();
            if (Globals.STRICT) {
                if (index - this.pollIndex.get() >= this.numElements) {
                    throw new OutOfMemoryException();
                }
            }

            int sanitizedIndex = sanitizeIndex(index);
            Object lock = LOCK_CACHE[sanitizedIndex % LOCK_CACHE_SIZE];

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
                    this.queue.putInt(sanitizedIndex << 2, value);
                    return;
                }
            }
        }
    }

    public int remaining() {
        return this.offerIndex.get() - this.pollIndex.get();
    }

    private int sanitizeIndex(int index) {
        if (index >= 0) {
            return index % this.numElements;
        }
        return (int) ((this.sanitizer + index) % this.numElements);
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
