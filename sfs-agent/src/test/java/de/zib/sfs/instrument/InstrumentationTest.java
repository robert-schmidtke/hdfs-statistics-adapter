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

        {
            File file = File.createTempFile("stream", null);

            // write a total of 1 MB
            FileOutputStream fos = new FileOutputStream(file);
            int writeByte = random.nextInt(Byte.MAX_VALUE);
            fos.write(writeByte);

            byte[] writeBuffer = new byte[1048575];
            for (int i = 0; i < writeBuffer.length; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            fos.write(writeBuffer);
            fos.close();
            assert (file.length() == 1048576);

            // read a total of 1 MB
            FileInputStream fis = new FileInputStream(file);
            int readByte = fis.read();
            assert (readByte == writeByte);

            byte[] readBuffer = new byte[1048575];
            int numRead = fis.read(readBuffer);
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
            int writeByte = random.nextInt(Byte.MAX_VALUE);
            writeFile.write(writeByte);

            byte[] writeBuffer = new byte[1048575];
            for (int i = 0; i < writeBuffer.length; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            writeFile.write(writeBuffer);
            writeFile.close();
            assert (file.length() == 1048576);

            // read a total of 1 MB
            RandomAccessFile readFile = new RandomAccessFile(file, "r");
            int readByte = readFile.read();
            assert (readByte == writeByte);

            byte[] readBuffer = new byte[1048575];
            int numRead = readFile.read(readBuffer);
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

            // write a total of 6 MB
            FileOutputStream fos = new FileOutputStream(file);
            FileChannel fco = fos.getChannel();

            byte[] writeBuffer = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }

            long numWritten = fco.write(ByteBuffer.wrap(writeBuffer));
            numWritten += fco.write(ByteBuffer.wrap(writeBuffer), 1048576);
            assert (numWritten == 2 * 1048576);

            numWritten = fco.write(new ByteBuffer[] {
                    ByteBuffer.wrap(writeBuffer), ByteBuffer.wrap(writeBuffer),
                    ByteBuffer.wrap(writeBuffer) });

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
            assert (numWritten == 4 * 1048576);

            fco.close();
            fos.close();

            // read a total of 6 MB
            FileInputStream fis = new FileInputStream(file);
            FileChannel fci = fis.getChannel();

            byte[] readBuffer = new byte[1048576];
            long numRead = fci.read(ByteBuffer.wrap(readBuffer));
            numRead += fci.read(ByteBuffer.wrap(readBuffer), 1048576);
            assert (numRead == 2 * 1048576);
            for (int i = 0; i < 1048576; ++i) {
                assert (readBuffer[i] == writeBuffer[i]);
            }

            byte[][] readBuffers = new byte[3][1048576];
            numRead = fci
                    .read(new ByteBuffer[] { ByteBuffer.wrap(readBuffers[0]),
                            ByteBuffer.wrap(readBuffers[1]),
                            ByteBuffer.wrap(readBuffers[2]) });

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
            assert (numRead == 4 * 1048576);
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
            FileChannel fco = writeFile.getChannel();

            MappedByteBuffer mbbo = fco.map(MapMode.READ_WRITE, 0, 2 * 1048576);

            byte b = (byte) random.nextInt(Byte.MAX_VALUE);
            mbbo.put(b); // 1 byte

            char c = (char) random.nextInt(Character.MAX_VALUE);
            mbbo.putChar(c); // 2 bytes

            double d = random.nextDouble();
            mbbo.putDouble(d); // 8 bytes

            float f = random.nextFloat();
            mbbo.putFloat(f); // 4 bytes

            int i = random.nextInt();
            mbbo.putInt(i); // 4 bytes

            long l = random.nextLong();
            mbbo.putLong(l); // 8 bytes

            short s = (short) random.nextInt(Short.MAX_VALUE);
            mbbo.putShort(s); // 2 bytes

            // fill the rest
            byte[] writeBuffer = new byte[1048547];
            for (int j = 0; j < writeBuffer.length; ++j) {
                writeBuffer[j] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            mbbo.put(writeBuffer);

            // write to a dummy file so we can map its buffer
            // this gives an extra 1 MB in the write statistics
            File dummyFile = File.createTempFile("from", null);
            RandomAccessFile dummyRaf = new RandomAccessFile(dummyFile, "rw");
            byte[] writeBuffer2 = new byte[1048576];
            for (int j = 0; j < writeBuffer2.length; ++j) {
                writeBuffer2[j] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            dummyRaf.write(writeBuffer2);
            dummyRaf.close();

            // this gives an extra 1 MB in the read statistics
            dummyRaf = new RandomAccessFile(dummyFile, "r");
            mbbo.put(dummyRaf.getChannel().map(MapMode.READ_ONLY, 0, 1048576));
            dummyRaf.close();

            fco.close();
            writeFile.close();
            assert (file.length() == 2 * 1048576);

            RandomAccessFile readFile = new RandomAccessFile(file, "r");
            FileChannel fci = readFile.getChannel();

            MappedByteBuffer mbbi = fci.map(MapMode.READ_ONLY, 0, 2 * 1048576);

            assert (b == mbbi.get());
            assert (c == mbbi.getChar());
            assert (d == mbbi.getDouble());
            assert (f == mbbi.getFloat());
            assert (i == mbbi.getInt());
            assert (l == mbbi.getLong());
            assert (s == mbbi.getShort());

            byte[] readBuffer = new byte[1048547];
            mbbi.get(readBuffer);
            for (int j = 0; j < readBuffer.length; ++j) {
                assert (readBuffer[j] == writeBuffer[j]);
            }

            byte[] readBuffer2 = new byte[1048576];
            mbbi.get(readBuffer2);
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

            // we opened the file 2 + 2 + 2 + 2 + 2 = 10 times, however the JVM
            // might
            // open a lot more files, especially during class loading, so no
            // exact estimation possible
            assertOperationCount(aggregates, OperationSource.JVM,
                    OperationCategory.OTHER, 10);

            // we wrote 1 + 1 + 6 + 2 + 1 = 11 MB, no slack
            // for the JVM
            assertOperationData(aggregates, OperationSource.JVM,
                    OperationCategory.WRITE, 11 * 1048576, 11 * 1048576);

            // we read 1 + 1 + 6 + 2 + 1 = 11 MB, allow 48K
            // slack for the JVM
            assertOperationData(aggregates, OperationSource.JVM,
                    OperationCategory.READ, 11 * 1048576,
                    11 * 1048576 + 48 * 1024);
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
