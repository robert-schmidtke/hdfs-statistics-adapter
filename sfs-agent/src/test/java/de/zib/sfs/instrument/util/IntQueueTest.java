/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.zib.sfs.instrument.util.IntQueue.OutOfMemoryException;

public class IntQueueTest {

    private static final int QUEUE_SIZE = 1024;

    IntQueue queue;

    @Before
    public void setUp() {
        this.queue = new IntQueue(QUEUE_SIZE);
    }

    @Test
    public void testOfferPoll() {
        Assert.assertEquals(0, this.queue.remaining());

        this.queue.offer(42);
        Assert.assertEquals(1, this.queue.remaining());

        Assert.assertEquals(42, this.queue.poll());
        Assert.assertEquals(0, this.queue.remaining());

        for (int i = 0; i < QUEUE_SIZE; ++i) {
            this.queue.offer(i);
        }
        Assert.assertEquals(QUEUE_SIZE, this.queue.remaining());

        for (int i = 0; i < QUEUE_SIZE; ++i) {
            Assert.assertEquals(i, this.queue.poll());
        }
        Assert.assertEquals(0, this.queue.remaining());
    }

    @Test
    public void testPollTooMany() {
        Assert.assertEquals(Integer.MIN_VALUE, this.queue.poll());
        this.queue.offer(42);
        Assert.assertEquals(42, this.queue.poll());
        Assert.assertEquals(Integer.MIN_VALUE, this.queue.poll());
    }

    @Test
    public void testOfferTooMany() {
        for (int i = 0; i < QUEUE_SIZE; ++i) {
            this.queue.offer(i);
        }
        Assert.assertEquals(QUEUE_SIZE, this.queue.remaining());

        try {
            this.queue.offer(42);
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
    public void testConcurrency() {
        long[] values = new long[QUEUE_SIZE];

        // queue holds indices into values
        for (int i = 0; i < QUEUE_SIZE; ++i) {
            values[i] = 0;
            this.queue.offer(i);
        }
        Assert.assertEquals(QUEUE_SIZE, this.queue.remaining());

        // each thread adds 1 + ... + QUEUE_SIZE to the total value of the array
        final long perThreadTotal = QUEUE_SIZE * (QUEUE_SIZE + 1L) / 2L;
        Callable<Void> c = new Callable<Void>() {
            @Override
            public Void call() {
                try {
                    for (int i = 1; i <= QUEUE_SIZE; ++i) {
                        int index;
                        do {
                            index = IntQueueTest.this.queue.poll();
                        } while (index == Integer.MIN_VALUE);

                        // non-atomic add should reveal concurrency issues
                        values[index] += i;

                        IntQueueTest.this.queue.offer(index);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                }
                return null;
            }
        };

        // QUEUE_SIZE threads
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < QUEUE_SIZE; ++i) {
            executor.submit(c);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                Assert.fail("Thread pool did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(QUEUE_SIZE, this.queue.remaining());

        // total value of array should be QUEUE_SIZE * (1 + ... + QUEUE_SIZE)
        final long expectedTotal = QUEUE_SIZE * perThreadTotal;
        long total = 0;
        for (long v : values) {
            total += v;
        }
        Assert.assertEquals(expectedTotal, total);
    }

    @Test
    public void testOverflow() {
        this.queue.setPollIndex(Integer.MAX_VALUE - QUEUE_SIZE - 10);
        this.queue.setOfferIndex(Integer.MAX_VALUE - QUEUE_SIZE - 10);
        Assert.assertEquals(0, this.queue.remaining());

        Callable<Void> c = new Callable<Void>() {
            @Override
            public Void call() {
                try {
                    for (int i = 1; i <= QUEUE_SIZE; ++i) {
                        IntQueueTest.this.queue.offer(i);
                        int v;
                        do {
                            v = IntQueueTest.this.queue.poll();
                        } while (v == Integer.MIN_VALUE);

                        Assert.assertTrue(v >= 1 && v <= QUEUE_SIZE);
                    }
                } catch (Throwable t) {
                    t.printStackTrace();
                    throw t;
                }
                return null;
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < QUEUE_SIZE; ++i) {
            executor.submit(c);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                Assert.fail("Thread pool did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        Assert.assertEquals(0, this.queue.remaining());
    }

}
