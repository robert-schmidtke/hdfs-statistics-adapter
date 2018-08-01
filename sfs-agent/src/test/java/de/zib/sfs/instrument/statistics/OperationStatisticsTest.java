/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.zib.sfs.instrument.util.MemoryPool;

public class OperationStatisticsTest {

    private Random rnd = null;

    @Before
    public void setUp() {
        System.setProperty("de.zib.sfs.operationStatistics.poolSize", "128");
        System.setProperty("de.zib.sfs.operationStatistics.lockCacheSize",
                "64");

        System.setProperty("de.zib.sfs.dataOperationStatistics.poolSize",
                "128");
        System.setProperty("de.zib.sfs.dataOperationStatistics.lockCacheSize",
                "64");

        System.setProperty("de.zib.sfs.readDataOperationStatistics.poolSize",
                "128");
        System.setProperty(
                "de.zib.sfs.readDataOperationStatistics.lockCacheSize", "64");

        rnd = new Random(0);
    }

    @Test
    public void testOperationStatistics() {
        // lock cache assumes divisibility by two
        Assert.assertEquals(0, OperationStatistics.SIZE % 2);
        Assert.assertEquals(0, DataOperationStatistics.SIZE % 2);
        Assert.assertEquals(0, ReadDataOperationStatistics.SIZE % 2);

        MemoryPool outerMp = null;
        for (int j = 0; j < 128; ++j) {
            MemoryPool innerMp = null;
            for (int i = 0; i < 128; ++i) {
                long startTime = rnd.nextLong();
                int fd = rnd.nextInt();
                long tid = rnd.nextLong();
                long address = OperationStatistics.getOperationStatistics(1000,
                        OperationSource.JVM, OperationCategory.OTHER, startTime,
                        startTime + 5, fd, tid);

                // multiples of 46 because OS is that big, index of pool
                // prepended
                Assert.assertEquals(((long) j << 32) | (i * 46), address);

                Assert.assertEquals(startTime - (startTime % 1000),
                        OperationStatistics.getTimeBin(address));
                Assert.assertEquals(OperationSource.JVM,
                        OperationStatistics.getSource(address));
                Assert.assertEquals(OperationCategory.OTHER,
                        OperationStatistics.getCategory(address));
                Assert.assertEquals(fd,
                        OperationStatistics.getFileDescriptor(address));
                Assert.assertEquals(5, OperationStatistics.getCpuTime(address));

                MemoryPool mp = OperationStatistics.getMemoryPool(address);
                int sanitizedAddress = OperationStatistics
                        .sanitizeAddress(address);
                Assert.assertEquals(startTime - (startTime % 1000),
                        OperationStatistics.getTimeBin(mp, sanitizedAddress));
                Assert.assertEquals(OperationSource.JVM,
                        OperationStatistics.getSource(mp, sanitizedAddress));
                Assert.assertEquals(OperationCategory.OTHER,
                        OperationStatistics.getCategory(mp, sanitizedAddress));
                Assert.assertEquals(fd, OperationStatistics
                        .getFileDescriptor(mp, sanitizedAddress));
                Assert.assertEquals(5,
                        OperationStatistics.getCpuTime(mp, sanitizedAddress));

                if (innerMp == null) {
                    innerMp = mp;
                } else {
                    Assert.assertEquals(innerMp, mp);
                }
            }

            if (outerMp == null) {
                outerMp = innerMp;
            } else {
                Assert.assertNotEquals(innerMp, outerMp);
            }
        }

        // return all addresses in the same order
        for (int j = 0; j < 128; ++j) {
            for (int i = 0; i < 128; ++i) {
                OperationStatistics
                        .returnOperationStatistics(((long) j << 32) | (i * 46));
            }
        }

        // now expect addresses returned in the other direction
        for (int j = 127; j >= 0; --j) {
            for (int i = 0; i < 128; ++i) {
                long startTime = System.nanoTime();
                long address = OperationStatistics.getOperationStatistics(1000,
                        OperationSource.JVM, OperationCategory.OTHER, startTime,
                        startTime + 5000000, 42, 43);

                // multiples of 46 because OS is that big, index of pool
                // prepended
                Assert.assertEquals(((long) j << 32) | (i * 46), address);
            }
        }
    }

    @Test
    public void testDataOperationStatistics() {
        MemoryPool outerMp = null;
        for (int j = 0; j < 128; ++j) {
            MemoryPool innerMp = null;
            for (int i = 0; i < 128; ++i) {
                long startTime = rnd.nextLong();
                int fd = rnd.nextInt();
                long tid = rnd.nextLong();
                long data = rnd.nextLong();
                long address = DataOperationStatistics
                        .getDataOperationStatistics(1000, OperationSource.JVM,
                                OperationCategory.WRITE, startTime,
                                startTime + 5, fd, tid, data);

                Assert.assertEquals((1L << 61) | ((long) j << 32) | (i * 54),
                        address);
                Assert.assertEquals(data,
                        DataOperationStatistics.getData(address));

                MemoryPool mp = OperationStatistics.getMemoryPool(address);
                int sanitizedAddress = OperationStatistics
                        .sanitizeAddress(address);
                Assert.assertEquals(data,
                        DataOperationStatistics.getData(mp, sanitizedAddress));

                if (innerMp == null) {
                    innerMp = mp;
                } else {
                    Assert.assertEquals(innerMp, mp);
                }
            }

            if (outerMp == null) {
                outerMp = innerMp;
            } else {
                Assert.assertNotEquals(innerMp, outerMp);
            }
        }

        // return all addresses in the same order
        for (int j = 0; j < 128; ++j) {
            for (int i = 0; i < 128; ++i) {
                OperationStatistics.returnOperationStatistics(
                        (1L << 61) | ((long) j << 32) | (i * 54));
            }
        }

        // now expect addresses returned in the other direction
        for (int j = 127; j >= 0; --j) {
            for (int i = 0; i < 128; ++i) {
                long startTime = System.nanoTime();
                long address = DataOperationStatistics
                        .getDataOperationStatistics(1000, OperationSource.JVM,
                                OperationCategory.WRITE, startTime,
                                startTime + 5000000, 42, 43, 42);

                Assert.assertEquals((1L << 61) | ((long) j << 32) | (i * 54),
                        address);
            }
        }
    }

    @Test
    public void testReadDataOperationStatistics() {
        MemoryPool outerMp = null;
        for (int j = 0; j < 128; ++j) {
            MemoryPool innerMp = null;
            for (int i = 0; i < 128; ++i) {
                long startTime = rnd.nextLong();
                int fd = rnd.nextInt();
                long tid = rnd.nextLong();
                long data = rnd.nextLong();
                boolean remote = rnd.nextBoolean();
                long address = ReadDataOperationStatistics
                        .getReadDataOperationStatistics(1000,
                                OperationSource.JVM, OperationCategory.READ,
                                startTime, startTime + 5, fd, tid, data,
                                remote);

                Assert.assertEquals((2L << 61) | ((long) j << 32) | (i * 78),
                        address);

                Assert.assertEquals(remote ? data : 0,
                        ReadDataOperationStatistics.getRemoteData(address));
                Assert.assertEquals(remote ? 1 : 0,
                        ReadDataOperationStatistics.getRemoteCount(address));

                MemoryPool mp = OperationStatistics.getMemoryPool(address);
                int sanitizedAddress = OperationStatistics
                        .sanitizeAddress(address);
                Assert.assertEquals(remote ? data : 0,
                        ReadDataOperationStatistics.getRemoteData(mp,
                                sanitizedAddress));
                Assert.assertEquals(remote ? 1 : 0, ReadDataOperationStatistics
                        .getRemoteCount(mp, sanitizedAddress));

                if (innerMp == null) {
                    innerMp = mp;
                } else {
                    Assert.assertEquals(innerMp, mp);
                }
            }

            if (outerMp == null) {
                outerMp = innerMp;
            } else {
                Assert.assertNotEquals(innerMp, outerMp);
            }
        }

        // return all addresses in the same order
        for (int j = 0; j < 128; ++j) {
            for (int i = 0; i < 128; ++i) {
                OperationStatistics.returnOperationStatistics(
                        (2L << 61) | ((long) j << 32) | (i * 78));
            }
        }

        // now expect addresses returned in the other direction
        for (int j = 127; j >= 0; --j) {
            for (int i = 0; i < 128; ++i) {
                long startTime = System.nanoTime();
                long address = ReadDataOperationStatistics
                        .getReadDataOperationStatistics(1000,
                                OperationSource.JVM, OperationCategory.READ,
                                startTime, startTime + 5000000, 42, 43, 42,
                                true);

                Assert.assertEquals((2L << 61) | ((long) j << 32) | (i * 78),
                        address);
            }
        }
    }

}
