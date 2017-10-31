/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.zib.sfs.instrument.util.MemoryPool.IllegalAddressException;
import de.zib.sfs.instrument.util.MemoryPool.OutOfMemoryException;

public class MemoryPoolTest {

    private static final int POOL_SIZE = 1024;

    MemoryPool pool;

    @Before
    public void setUp() {
        this.pool = new MemoryPool(8 * POOL_SIZE, 8);
    }

    @Test
    public void testAllocFree() {
        Assert.assertEquals(POOL_SIZE, this.pool.remaining());

        int address = this.pool.alloc();
        Assert.assertEquals(0, address);
        Assert.assertEquals(POOL_SIZE - 1, this.pool.remaining());

        this.pool.free(address);
        Assert.assertEquals(POOL_SIZE, this.pool.remaining());
    }

    @Test
    public void testAllocTooMany() {
        int address = -1;
        for (int i = 0; i < POOL_SIZE; ++i) {
            address = this.pool.alloc();
        }
        Assert.assertEquals(0, this.pool.remaining());

        try {
            this.pool.alloc();
            if (Globals.STRICT) {
                Assert.fail("Expected exception.");
            }
        } catch (OutOfMemoryException e) {
            if (!Globals.STRICT) {
                Assert.fail("Did not expect exception.");
            }
        }
    }

    @Test
    public void testFreeTooMany() {
        int address = this.pool.alloc();
        this.pool.free(address);
        try {
            this.pool.free(address);
            if (Globals.STRICT) {
                Assert.fail("Expected exception.");
            }
        } catch (IllegalAddressException e) {
            if (!Globals.STRICT) {
                Assert.fail("Did not expect exception.");
            }
        }
    }

    @Test
    public void testConcurrency() {
        final int COUNT = 1048576;
        final long SUM = COUNT * (COUNT + 1L) / 2L;

        Callable<Long> c = new Callable<Long>() {
            @Override
            public Long call() {
                try {
                    int address = MemoryPoolTest.this.pool.alloc();
                    MemoryPoolTest.this.pool.pool.putLong(address, 0);
                    for (long i = 1; i <= COUNT; ++i) {
                        MemoryPoolTest.this.pool.pool.putLong(address,
                                MemoryPoolTest.this.pool.pool.getLong(address)
                                        + i);
                    }
                    long result = MemoryPoolTest.this.pool.pool
                            .getLong(address);
                    MemoryPoolTest.this.pool.free(address);
                    return result;
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<Long>> futures = new ArrayList<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; ++i) {
            futures.add(executor.submit(c));
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                Assert.fail("Thread pool did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        for (Future<Long> future : futures) {
            try {
                Assert.assertEquals(SUM, future.get().longValue());
            } catch (InterruptedException | ExecutionException e) {
                Assert.fail(e.getMessage());
            }
        }

        Assert.assertEquals(POOL_SIZE, this.pool.remaining());
    }

    @Test
    public void testOverflow() {
        this.pool.setAllocIndex(Integer.MAX_VALUE - POOL_SIZE - 10);
        this.pool.setFreeIndex(Integer.MAX_VALUE - 10);
        testConcurrency();
        testConcurrency();

        this.pool.setAllocIndex(Integer.MAX_VALUE - POOL_SIZE - 10);
        this.pool.setFreeIndex(Integer.MAX_VALUE - 10);
        testConcurrency();
        testAllocTooMany();
    }

}
