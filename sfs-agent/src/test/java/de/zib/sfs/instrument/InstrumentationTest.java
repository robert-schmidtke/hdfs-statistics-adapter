/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;

/**
 * Basic test cases that cover the wrapped methods of FileInputStream,
 * FileOutputStream, RandomAccessFile and FileChannelImpl.
 * 
 * @author robert
 *
 */
public class InstrumentationTest {

    public static void main(String[] args)
            throws IOException, InterruptedException {
        try {
            // try to get the aggregator
            LiveOperationStatisticsAggregator aggregator = LiveOperationStatisticsAggregator.instance;

            // we got it, now wait for all JVM related I/O to settle
            Thread.sleep(5000);

            // clear all statistics so we can check them later
            aggregator.reset();
        } catch (NoClassDefFoundError e) {
            // we're not instrumented, discard
        }

        final Random random = new Random();

        // count operations and data
        int openOperations = 0;
        long readBytes = 0, writeBytes = 0;

        {
            File file = File.createTempFile("stream", null);

            // write a total of 1 MB
            FileOutputStream fos = new FileOutputStream(file);
            ++openOperations;

            int writeByte = random.nextInt(Byte.MAX_VALUE);
            fos.write(writeByte);
            ++writeBytes;

            byte[] writeBuffer = new byte[1048575];
            for (int i = 0; i < writeBuffer.length; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            fos.write(writeBuffer);
            writeBytes += 1048575;

            fos.close();
            assert (file.length() == 1048576);

            // read a total of 1 MB
            FileInputStream fis = new FileInputStream(file);
            ++openOperations;

            int readByte = fis.read();
            ++readBytes;
            assert (readByte == writeByte);

            byte[] readBuffer = new byte[1048575];
            int numRead = fis.read(readBuffer);
            readBytes += 1048575;
            assert (numRead == 1048575);
            for (int i = 0; i < 1048575; ++i) {
                assert (writeBuffer[i] == readBuffer[i]);
            }
            numRead = fis.read();
            assert (numRead == -1);
            fis.close();

            file.delete();
        }

        {
            File file = File.createTempFile("random", null);

            // write a total of 1 MB
            RandomAccessFile writeFile = new RandomAccessFile(file, "rw");
            ++openOperations;

            int writeByte = random.nextInt(Byte.MAX_VALUE);
            writeFile.write(writeByte);
            ++writeBytes;

            byte[] writeBuffer = new byte[1048575];
            for (int i = 0; i < writeBuffer.length; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            writeFile.write(writeBuffer);
            writeBytes += 1048575;

            writeFile.close();
            assert (file.length() == 1048576);

            // read a total of 1 MB
            RandomAccessFile readFile = new RandomAccessFile(file, "r");
            ++openOperations;

            int readByte = readFile.read();
            ++readBytes;
            assert (readByte == writeByte);

            byte[] readBuffer = new byte[1048575];
            int numRead = readFile.read(readBuffer);
            readBytes += 1048575;
            assert (numRead == 1048575);
            for (int i = 0; i < 1048575; ++i) {
                assert (writeBuffer[i] == readBuffer[i]);
            }
            numRead = readFile.read();
            assert (numRead == -1);
            readFile.close();

            file.delete();
        }

        {
            File file = File.createTempFile("channel", null);

            // write a total of 8 MB
            FileOutputStream fos = new FileOutputStream(file);
            ++openOperations;

            FileChannel fco = fos.getChannel();

            byte[] writeBuffer = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }

            long numWritten = fco.write(ByteBuffer.wrap(writeBuffer));
            writeBytes += 1048576;

            ByteBuffer allocatedWriteBuffer = ByteBuffer.allocate(1048576);
            allocatedWriteBuffer.put(writeBuffer);
            allocatedWriteBuffer.position(0);
            numWritten += fco.write(allocatedWriteBuffer, 1048576);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;

            ByteBuffer allocatedDirectWriteBuffer = ByteBuffer
                    .allocateDirect(1048576);
            allocatedDirectWriteBuffer.put(writeBuffer);
            allocatedDirectWriteBuffer.position(0);
            numWritten += fco.write(allocatedDirectWriteBuffer, 2 * 1048576);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;

            numWritten += fco.write(ByteBuffer.wrap(writeBuffer), 1048576);
            writeBytes += 1048576;

            numWritten += fco.write(new ByteBuffer[] {
                    ByteBuffer.wrap(writeBuffer), ByteBuffer.wrap(writeBuffer),
                    ByteBuffer.wrap(writeBuffer) });
            writeBytes += 3 * 1048576;

            numWritten += fco.transferFrom(new ReadableByteChannel() {
                boolean open = true;

                @Override
                public boolean isOpen() {
                    return open;
                }

                @Override
                public void close() throws IOException {
                    open = false;
                }

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    // produce random bytes from this channel
                    byte[] src = new byte[dst.remaining()];
                    for (int i = 0; i < src.length; ++i) {
                        src[i] = (byte) random.nextInt(Byte.MAX_VALUE);
                    }
                    dst.put(src, 0, src.length);
                    return src.length;
                }
            }, 0, 1048576);
            writeBytes += 1048576;
            assert (numWritten == 8 * 1048576);

            fco.close();
            fos.close();

            // read a total of 8 MB
            FileInputStream fis = new FileInputStream(file);
            ++openOperations;

            FileChannel fci = fis.getChannel();

            byte[] readBuffer = new byte[1048576];
            long numRead = fci.read(ByteBuffer.wrap(readBuffer));
            readBytes += 1048576;

            ByteBuffer allocatedReadBuffer = ByteBuffer.allocate(1048576);
            numRead += fci.read(allocatedReadBuffer, 1048576);
            fci.position(fci.position() + 1048576);
            readBytes += 1048576;

            allocatedWriteBuffer.position(0);
            allocatedReadBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (allocatedWriteBuffer.get() == allocatedReadBuffer
                        .get());
            }

            ByteBuffer allocatedDirectReadBuffer = ByteBuffer
                    .allocateDirect(1048576);
            numRead += fci.read(allocatedDirectReadBuffer, 2 * 1048576);
            fci.position(fci.position() + 1048576);
            readBytes += 1048576;

            allocatedDirectWriteBuffer.position(0);
            allocatedDirectReadBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (allocatedDirectWriteBuffer
                        .get() == allocatedDirectReadBuffer.get());
            }

            numRead += fci.read(ByteBuffer.wrap(readBuffer), 1048576);
            readBytes += 1048576;
            for (int i = 0; i < 1048576; ++i) {
                assert (readBuffer[i] == writeBuffer[i]);
            }

            byte[][] readBuffers = new byte[3][1048576];
            numRead += fci
                    .read(new ByteBuffer[] { ByteBuffer.wrap(readBuffers[0]),
                            ByteBuffer.wrap(readBuffers[1]),
                            ByteBuffer.wrap(readBuffers[2]) });
            readBytes += 3 * 1048576;

            numRead += fci.transferTo(0, 1048576, new WritableByteChannel() {
                boolean open = true;

                @Override
                public boolean isOpen() {
                    return open;
                }

                @Override
                public void close() throws IOException {
                    open = false;
                }

                @Override
                public int write(ByteBuffer src) throws IOException {
                    // discard everything
                    byte[] dst = new byte[src.remaining()];
                    src.get(dst, 0, dst.length);
                    return dst.length;
                }
            });
            readBytes += 1048576;
            assert (numRead == 8 * 1048576);
            numRead = fci.read(ByteBuffer.wrap(readBuffer));
            assert (numRead == -1);
            fci.close();
            fis.close();

            for (int i = 0; i < 3; ++i) {
                for (int j = 0; j < 1048576; ++j) {
                    assert (readBuffers[i][j] == writeBuffer[j]);
                }
            }

            file.delete();
        }

        {
            File file = File.createTempFile("channel", null);

            RandomAccessFile writeFile = new RandomAccessFile(file, "rw");
            ++openOperations;

            FileChannel fco = writeFile.getChannel();

            MappedByteBuffer mbbo = fco.map(MapMode.READ_WRITE, 0, 2 * 1048576);

            byte b = (byte) random.nextInt(Byte.MAX_VALUE);
            mbbo.put(b); // 1 byte
            ++writeBytes;

            char c = (char) random.nextInt(Character.MAX_VALUE);
            mbbo.putChar(c); // 2 bytes
            writeBytes += 2;

            double d = random.nextDouble();
            mbbo.putDouble(d); // 8 bytes
            writeBytes += 8;

            float f = random.nextFloat();
            mbbo.putFloat(f); // 4 bytes
            writeBytes += 4;

            int i = random.nextInt();
            mbbo.putInt(i); // 4 bytes
            writeBytes += 4;

            long l = random.nextLong();
            mbbo.putLong(l); // 8 bytes
            writeBytes += 8;

            short s = (short) random.nextInt(Short.MAX_VALUE);
            mbbo.putShort(s); // 2 bytes
            writeBytes += 2;

            // fill the rest
            byte[] writeBuffer = new byte[1048547];
            for (int j = 0; j < writeBuffer.length; ++j) {
                writeBuffer[j] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            mbbo.put(writeBuffer);
            writeBytes += 1048547;

            // write to a dummy file so we can map its buffer
            // this gives an extra 1 MB in the write statistics
            File dummyFile = File.createTempFile("from", null);
            RandomAccessFile dummyRaf = new RandomAccessFile(dummyFile, "rw");
            ++openOperations;
            byte[] writeBuffer2 = new byte[1048576];
            for (int j = 0; j < writeBuffer2.length; ++j) {
                writeBuffer2[j] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            dummyRaf.write(writeBuffer2);
            writeBytes += 1048576;

            dummyRaf.close();

            // this gives an extra 1 MB in the read statistics
            dummyRaf = new RandomAccessFile(dummyFile, "r");
            ++openOperations;

            mbbo.put(dummyRaf.getChannel().map(MapMode.READ_ONLY, 0, 1048576));
            writeBytes += 1048576;
            readBytes += 1048576;

            dummyRaf.close();

            fco.close();
            writeFile.close();
            assert (file.length() == 2 * 1048576);

            RandomAccessFile readFile = new RandomAccessFile(file, "r");
            ++openOperations;

            FileChannel fci = readFile.getChannel();

            MappedByteBuffer mbbi = fci.map(MapMode.READ_ONLY, 0, 2 * 1048576);

            assert (b == mbbi.get());
            ++readBytes;
            assert (c == mbbi.getChar());
            readBytes += 2;
            assert (d == mbbi.getDouble());
            readBytes += 8;
            assert (f == mbbi.getFloat());
            readBytes += 4;
            assert (i == mbbi.getInt());
            readBytes += 4;
            assert (l == mbbi.getLong());
            readBytes += 8;
            assert (s == mbbi.getShort());
            readBytes += 2;

            byte[] readBuffer = new byte[1048547];
            mbbi.get(readBuffer);
            readBytes += 1048547;

            for (int j = 0; j < readBuffer.length; ++j) {
                assert (readBuffer[j] == writeBuffer[j]);
            }

            byte[] readBuffer2 = new byte[1048576];
            mbbi.get(readBuffer2);
            readBytes += 1048576;

            for (int j = 0; j < readBuffer2.length; ++j) {
                assert (readBuffer2[j] == writeBuffer2[j]);
            }

            fci.close();
            readFile.close();

            file.delete();
        }

        try {
            // same as above
            LiveOperationStatisticsAggregator aggregator = LiveOperationStatisticsAggregator.instance;
            Thread.sleep(5000);

            // get statistics and check them
            List<NavigableMap<Long, OperationStatistics>> aggregates = aggregator
                    .getAggregates();

            // no SFS involved here
            for (OperationCategory category : OperationCategory.values()) {
                assert (aggregates
                        .get(LiveOperationStatisticsAggregator
                                .getUniqueIndex(OperationSource.SFS, category))
                        .size() == 0);
            }

            // we opened the file a few times, however the JVM might open a lot
            // more files, especially during class loading, so no exact
            // estimation possible
            assertOperationCount(aggregates, OperationSource.JVM,
                    OperationCategory.OTHER, openOperations);

            // no slack for the JVM
            assertOperationData(aggregates, OperationSource.JVM,
                    OperationCategory.WRITE, writeBytes, writeBytes);

            // allow 48K slack for the JVM
            assertOperationData(aggregates, OperationSource.JVM,
                    OperationCategory.READ, readBytes, readBytes + 48 * 1024);
        } catch (NoClassDefFoundError e) {
            // we're not instrumented, discard
        }

    }

    private static void assertOperationCount(
            List<NavigableMap<Long, OperationStatistics>> aggregates,
            OperationSource source, OperationCategory category, long atLeast) {
        Map<Long, OperationStatistics> operations = aggregates
                .get(LiveOperationStatisticsAggregator.getUniqueIndex(source,
                        category));
        long operationCount = 0;
        for (OperationStatistics os : operations.values()) {
            operationCount += os.getCount();
        }
        assert (operationCount >= atLeast) : ("actual " + operationCount
                + " vs. " + atLeast + " at least expected " + source + "/"
                + category + " operation count");
    }

    private static void assertOperationData(
            List<NavigableMap<Long, OperationStatistics>> aggregates,
            OperationSource source, OperationCategory category, long atLeast,
            long atMost) {
        Map<Long, OperationStatistics> operations = aggregates
                .get(LiveOperationStatisticsAggregator.getUniqueIndex(source,
                        category));
        long operationData = 0;
        for (OperationStatistics os : operations.values()) {
            assert (os instanceof DataOperationStatistics);
            operationData += ((DataOperationStatistics) os).getData();
        }
        assert (operationData >= atLeast
                && operationData <= atMost) : ("actual " + operationData
                        + " vs. " + atLeast + " at least / " + atMost
                        + " at most expected " + source + "/" + category
                        + " operation data");
    }
}
