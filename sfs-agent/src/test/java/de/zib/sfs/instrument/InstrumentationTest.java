/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.zib.sfs.instrument.statistics.DataOperationStatistics;
import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.ReadDataOperationStatistics;

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
        final Random random = new Random();

        // count operations and data
        int openOperations = 0;
        long readBytes = 0, writeBytes = 0;

        {
            File file = File.createTempFile("stream", null);

            // Write

            FileOutputStream fos = new FileOutputStream(file);
            ++openOperations;

            // use single byte writes
            byte[] writeBuffer = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
                fos.write(writeBuffer[i]);
            }
            writeBytes += 1048576;

            // use simple array write
            fos.write(writeBuffer);
            writeBytes += 1048576;

            // use offset/length array write
            fos.write(writeBuffer, 0, writeBuffer.length);
            writeBytes += 1048576;

            fos.close();
            assert (file.length() == 3 * 1048576);

            // Read

            FileInputStream fis = new FileInputStream(file);
            ++openOperations;

            // use single byte reads
            for (int i = 0; i < 1048576; ++i) {
                assert (fis.read() == writeBuffer[i]);
            }
            readBytes += 1048576;

            // use simple array read
            byte[] readBuffer = new byte[1048576];
            int numRead = fis.read(readBuffer);
            readBytes += 1048576;
            assert (numRead == 1048576);
            for (int i = 0; i < 1048576; ++i) {
                assert (writeBuffer[i] == readBuffer[i]);
            }

            // use offset/length array read
            numRead = fis.read(readBuffer, 0, readBuffer.length);
            readBytes += 1048576;
            assert (numRead == 1048576);
            for (int i = 0; i < 1048576; ++i) {
                assert (writeBuffer[i] == readBuffer[i]);
            }

            numRead = fis.read();
            assert (numRead == -1);
            fis.close();

            file.delete();
        }

        {
            File file = File.createTempFile("random", null);

            // Write

            RandomAccessFile writeFile = new RandomAccessFile(file, "rw");
            ++openOperations;

            byte[] writeBuffer = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
                writeFile.write(writeBuffer[i]);
            }
            writeBytes += 1048576;

            writeFile.write(writeBuffer);
            writeBytes += 1048576;

            writeFile.write(writeBuffer, 0, writeBuffer.length);
            writeBytes += 1048576;

            boolean[] bools = new boolean[1048576];
            for (int i = 0; i < 1048576; ++i) {
                bools[i] = random.nextBoolean();
                writeFile.writeBoolean(bools[i]);
            }
            writeBytes += 1048576;

            byte[] bytes = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                bytes[i] = (byte) random.nextInt(Byte.MAX_VALUE);
                writeFile.writeByte(bytes[i]);
            }
            writeBytes += 1048576;

            String string = new String(bytes);
            writeFile.writeBytes(string);
            writeBytes += 1048576;

            char[] chars = new char[524288];
            for (int i = 0; i < 524288; ++i) {
                chars[i] = (char) random.nextInt(Character.MAX_VALUE);
                writeFile.writeChar(chars[i]);
            }
            writeBytes += 1048576;

            double[] doubles = new double[131072];
            for (int i = 0; i < 131072; ++i) {
                doubles[i] = random.nextDouble();
                writeFile.writeDouble(doubles[i]);
            }
            writeBytes += 1048576;

            float[] floats = new float[262144];
            for (int i = 0; i < 262144; ++i) {
                floats[i] = random.nextFloat();
                writeFile.writeFloat(floats[i]);
            }
            writeBytes += 1048576;

            int[] ints = new int[262144];
            for (int i = 0; i < 262144; ++i) {
                ints[i] = random.nextInt();
                writeFile.writeInt(ints[i]);
            }
            writeBytes += 1048576;

            long[] longs = new long[131072];
            for (int i = 0; i < 131072; ++i) {
                longs[i] = random.nextLong();
                writeFile.writeLong(longs[i]);
            }
            writeBytes += 1048576;

            short[] shorts = new short[524288];
            for (int i = 0; i < 524288; ++i) {
                shorts[i] = (short) random.nextInt(Short.MAX_VALUE);
                writeFile.writeShort(shorts[i]);
            }
            writeBytes += 1048576;

            writeFile.writeChars(string.substring(0, 524288));
            writeBytes += 1048576;

            // 64K restriction on UTF-8 string length
            // so use 32 * 32K strings
            int utf8StringLength = 0;
            for (int i = 0; i < 32; ++i) {
                String s = string.substring(i * 32 * 1024, (i + 1) * 32 * 1024);

                // as per DataOutputStream.java
                for (int j = 0; j < s.length(); ++j) {
                    char c = s.charAt(j);
                    if ((c >= 0x0001) && (c <= 0x007F)) {
                        ++utf8StringLength;
                    } else if (c > 0x07FF) {
                        utf8StringLength += 3;
                    } else {
                        utf8StringLength += 2;
                    }
                }

                writeFile.writeUTF(s);
            }
            writeBytes += 1048576;

            writeFile.close();
            // 2 bytes extra per UTF write
            assert (file.length() == 13 * 1048576 + utf8StringLength + 2 * 32);

            // Read

            RandomAccessFile readFile = new RandomAccessFile(file, "r");
            ++openOperations;

            for (int i = 0; i < 1048576; ++i) {
                assert (readFile.read() == writeBuffer[i]);
            }
            readBytes += 1048576;

            byte[] readBuffer = new byte[1048576];
            int numRead = readFile.read(readBuffer);
            readBytes += 1048576;
            assert (numRead == 1048576);
            for (int i = 0; i < 1048576; ++i) {
                assert (writeBuffer[i] == readBuffer[i]);
            }

            numRead = readFile.read(readBuffer, 0, readBuffer.length);
            readBytes += 1048576;
            assert (numRead == 1048576);
            for (int i = 0; i < 1048576; ++i) {
                assert (writeBuffer[i] == readBuffer[i]);
            }

            for (int i = 0; i < 1048576; ++i) {
                assert (readFile.readBoolean() == bools[i]);
            }
            readBytes += 1048576;

            for (int i = 0; i < 1048576; ++i) {
                assert (readFile.readByte() == bytes[i]);
            }
            readBytes += 1048576;

            readFile.readFully(readBuffer);
            readBytes += 1048576;
            assert (new String(readBuffer).equals(string));

            for (int i = 0; i < 524288; ++i) {
                assert (readFile.readChar() == chars[i]);
            }
            readBytes += 1048576;

            for (int i = 0; i < 131072; ++i) {
                assert (readFile.readDouble() == doubles[i]);
            }
            readBytes += 1048576;

            for (int i = 0; i < 262144; ++i) {
                assert (readFile.readFloat() == floats[i]);
            }
            readBytes += 1048576;

            for (int i = 0; i < 262144; ++i) {
                assert (readFile.readInt() == ints[i]);
            }
            readBytes += 1048576;

            for (int i = 0; i < 131072; ++i) {
                assert (readFile.readLong() == longs[i]);
            }
            readBytes += 1048576;

            for (int i = 0; i < 524288; ++i) {
                assert (readFile.readShort() == shorts[i]);
            }
            readBytes += 1048576;

            readFile.readFully(readBuffer, 0, readBuffer.length);
            readBytes += 1048576;
            char[] characters = new char[524288];
            for (int i = 0; i < 524288; ++i) {
                characters[i] = (char) ((readBuffer[2 * i] << 8)
                        | readBuffer[2 * i + 1]);
            }
            assert (new String(characters).equals(string.substring(0, 524288)));

            for (int i = 0; i < 32; ++i) {
                assert (readFile.readUTF().equals(
                        string.substring(i * 32 * 1024, (i + 1) * 32 * 1024)));
            }
            readBytes += 1048576;

            numRead = readFile.read();
            assert (numRead == -1);
            readFile.close();

            file.delete();
        }

        {
            File file = File.createTempFile("channel", null);

            // Write

            FileOutputStream fos = new FileOutputStream(file);
            ++openOperations;

            FileChannel fco = fos.getChannel();

            byte[] writeBuffer = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }

            // set up 3 write buffers
            ByteBuffer wrappedWriteBuffer = ByteBuffer.wrap(writeBuffer);
            ByteBuffer allocatedWriteBuffer = ByteBuffer.allocate(1048576);
            allocatedWriteBuffer.put(writeBuffer);
            allocatedWriteBuffer.position(0);
            ByteBuffer allocatedDirectWriteBuffer = ByteBuffer
                    .allocateDirect(1048576);
            allocatedDirectWriteBuffer.put(writeBuffer);
            allocatedDirectWriteBuffer.position(0);

            // write all 3 buffers
            long numWritten = fco.write(wrappedWriteBuffer);
            wrappedWriteBuffer.position(0);
            writeBytes += 1048576;
            numWritten += fco.write(allocatedWriteBuffer);
            allocatedWriteBuffer.position(0);
            writeBytes += 1048576;
            numWritten += fco.write(allocatedDirectWriteBuffer);
            allocatedDirectWriteBuffer.position(0);
            writeBytes += 1048576;
            // fco is now 3 MB

            // write all 3 buffers using offsets
            numWritten += fco.write(wrappedWriteBuffer, 3 * 1048576);
            wrappedWriteBuffer.position(0);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;
            numWritten += fco.write(allocatedWriteBuffer, 4 * 1048576);
            allocatedWriteBuffer.position(0);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;
            numWritten += fco.write(allocatedDirectWriteBuffer, 5 * 1048576);
            allocatedDirectWriteBuffer.position(0);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;
            // fco is now 6 MB

            // write all 3 buffers using an array of them
            numWritten += fco.write(new ByteBuffer[] { wrappedWriteBuffer,
                    allocatedWriteBuffer, allocatedDirectWriteBuffer });
            writeBytes += 3 * 1048576;
            // fco is now 9 MB

            // write to a dummy file so we can map its buffer
            // this gives an extra 1 MB in the write statistics
            File dummyFile = File.createTempFile("dummy", null);
            RandomAccessFile dummyRaf = new RandomAccessFile(dummyFile, "rw");
            ++openOperations;
            dummyRaf.write(writeBuffer);
            writeBytes += 1048576;

            dummyRaf.close();

            dummyRaf = new RandomAccessFile(dummyFile, "r");
            ++openOperations;
            MappedByteBuffer mappedByteBuffer = dummyRaf.getChannel()
                    .map(MapMode.READ_ONLY, 0, 1048576);

            // use regular write
            numWritten += fco.write(mappedByteBuffer);
            writeBytes += 1048576;
            readBytes += 1048576;
            mappedByteBuffer.position(0);
            // fco is now 10 MB

            // use write with offset
            numWritten += fco.write(mappedByteBuffer, 10 * 1048576);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;
            readBytes += 1048576;
            mappedByteBuffer.position(0);
            // fco is now 11 MB

            // use array write
            numWritten += fco.write(new ByteBuffer[] { mappedByteBuffer });
            writeBytes += 1048576;
            readBytes += 1048576;
            mappedByteBuffer.position(0);
            // fco is now 12 MB

            // use transfer from file, incurring another 1 MB of reads
            numWritten += fco.transferFrom(dummyRaf.getChannel(), 12 * 1048576,
                    1048576);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;
            readBytes += 1048576;
            // fco is now 13 MB

            dummyRaf.close();

            // use transfer from arbitrary source
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
            }, 13 * 1048576, 1048576);
            fco.position(fco.position() + 1048576);
            writeBytes += 1048576;
            // fco is now 14 MB
            assert (numWritten == 14 * 1048576);

            fco.close();
            fos.close();
            assert (file.length() == 14 * 1048576);

            // Read

            FileInputStream fis = new FileInputStream(file);
            ++openOperations;

            FileChannel fci = fis.getChannel();

            byte[] readBuffer = new byte[1048576];

            // set up 3 read buffers
            ByteBuffer wrappedReadBuffer = ByteBuffer.wrap(readBuffer);
            ByteBuffer allocatedReadBuffer = ByteBuffer.allocate(1048576);
            ByteBuffer allocatedDirectReadBuffer = ByteBuffer.allocate(1048576);

            // read all 3 buffers
            long numRead = fci.read(wrappedReadBuffer);
            wrappedReadBuffer.position(0);
            readBytes += 1048576;
            numRead += fci.read(allocatedReadBuffer);
            allocatedReadBuffer.position(0);
            readBytes += 1048576;
            numRead += fci.read(allocatedDirectReadBuffer);
            allocatedDirectReadBuffer.position(0);
            readBytes += 1048576;
            // fci is now 3 MB

            // read all 3 buffers using offsets
            numRead += fci.read(wrappedReadBuffer, 3 * 1048576);
            wrappedReadBuffer.position(0);
            fci.position(fci.position() + 1048576);
            readBytes += 1048576;
            numRead += fci.read(allocatedReadBuffer, 4 * 1048576);
            allocatedReadBuffer.position(0);
            fci.position(fci.position() + 1048576);
            readBytes += 1048576;
            numRead += fci.read(allocatedDirectReadBuffer, 5 * 1048576);
            allocatedDirectReadBuffer.position(0);
            fci.position(fci.position() + 1048576);
            readBytes += 1048576;
            // fci is now 6 MB

            // verify contents
            wrappedWriteBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (wrappedWriteBuffer.get() == wrappedReadBuffer.get());
            }
            allocatedWriteBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (allocatedWriteBuffer.get() == allocatedReadBuffer
                        .get());
            }
            allocatedDirectWriteBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (allocatedDirectWriteBuffer
                        .get() == allocatedDirectReadBuffer.get());
            }

            // read all 3 buffers using an array of them
            wrappedReadBuffer.position(0);
            allocatedReadBuffer.position(0);
            allocatedDirectReadBuffer.position(0);
            numRead += fci.read(new ByteBuffer[] { wrappedReadBuffer,
                    allocatedReadBuffer, allocatedDirectReadBuffer });
            readBytes += 3 * 1048576;
            // fci is now 9 MB

            // verify contents
            wrappedWriteBuffer.position(0);
            wrappedReadBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (wrappedWriteBuffer.get() == wrappedReadBuffer.get());
            }
            allocatedWriteBuffer.position(0);
            allocatedReadBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (allocatedWriteBuffer.get() == allocatedReadBuffer
                        .get());
            }
            allocatedDirectWriteBuffer.position(0);
            allocatedDirectReadBuffer.position(0);
            for (int i = 0; i < 1048576; ++i) {
                assert (allocatedDirectWriteBuffer
                        .get() == allocatedDirectReadBuffer.get());
            }

            // read content back to file
            dummyRaf = new RandomAccessFile(dummyFile, "rw");
            ++openOperations;
            mappedByteBuffer = dummyRaf.getChannel().map(MapMode.READ_WRITE, 0,
                    1048576);

            // use regular read
            numRead += fci.read(mappedByteBuffer);
            writeBytes += 1048576;
            readBytes += 1048576;
            mappedByteBuffer.position(0);
            // fci is now 10 MB

            // use read with offset
            numRead += fci.read(mappedByteBuffer, 10 * 1048576);
            mappedByteBuffer.position(0);
            fci.position(fci.position() + 1048576);
            writeBytes += 1048576;
            readBytes += 1048576;
            // fci is now 11 MB

            // use array read
            numRead += fci.read(new ByteBuffer[] { mappedByteBuffer });
            writeBytes += 1048576;
            readBytes += 1048576;
            mappedByteBuffer.position(0);
            // fci is now 12 MB

            // use transfer to file
            numRead += fci.transferTo(12 * 1048576, 1048576,
                    dummyRaf.getChannel());
            fci.position(fci.position() + 1048576);
            writeBytes += 1048576;
            readBytes += 1048576;
            // fci is now 13 MB

            dummyRaf.close();

            numRead += fci.transferTo(13 * 1048576, 1048576,
                    new WritableByteChannel() {
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
            fci.position(fci.position() + 1048576);
            // fci is now 14 MB
            assert (numRead == 14 * 1048576);

            wrappedReadBuffer.position(0);
            numRead = fci.read(wrappedReadBuffer);
            assert (numRead == -1);
            fci.close();
            fis.close();

            file.delete();
        }

        {
            File file = File.createTempFile("channel", null);

            // Write

            RandomAccessFile writeFile = new RandomAccessFile(file, "rw");
            ++openOperations;

            FileChannel fco = writeFile.getChannel();

            MappedByteBuffer mbbo = fco.map(MapMode.READ_WRITE, 0,
                    20 * 1048576);

            int index = 0;

            byte[] bytes = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                bytes[i] = (byte) random.nextInt(Byte.MAX_VALUE);
                mbbo.put(bytes[i]);
                index += 1;
                mbbo.put(index, bytes[i]);
                index += 1;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 2 MB

            char[] chars = new char[524288];
            for (int i = 0; i < 524288; ++i) {
                chars[i] = (char) random.nextInt(Character.MAX_VALUE);
                mbbo.putChar(chars[i]);
                index += 2;
                mbbo.putChar(index, chars[i]);
                index += 2;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 4 MB

            double[] doubles = new double[131072];
            for (int i = 0; i < 131072; ++i) {
                doubles[i] = random.nextDouble();
                mbbo.putDouble(doubles[i]);
                index += 8;
                mbbo.putDouble(index, doubles[i]);
                index += 8;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 6 MB

            float[] floats = new float[262144];
            for (int i = 0; i < 262144; ++i) {
                floats[i] = random.nextFloat();
                mbbo.putFloat(floats[i]);
                index += 4;
                mbbo.putFloat(index, floats[i]);
                index += 4;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 8 MB

            int[] ints = new int[262144];
            for (int i = 0; i < 262144; ++i) {
                ints[i] = random.nextInt();
                mbbo.putInt(ints[i]);
                index += 4;
                mbbo.putInt(index, ints[i]);
                index += 4;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 10 MB

            long[] longs = new long[131072];
            for (int i = 0; i < 131072; ++i) {
                longs[i] = random.nextLong();
                mbbo.putLong(longs[i]);
                index += 8;
                mbbo.putLong(index, longs[i]);
                index += 8;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 12 MB

            short[] shorts = new short[524288];
            for (int i = 0; i < 524288; ++i) {
                shorts[i] = (short) random.nextInt(Short.MAX_VALUE);
                mbbo.putShort(shorts[i]);
                index += 2;
                mbbo.putShort(index, shorts[i]);
                index += 2;
                mbbo.position(index);
            }
            writeBytes += 2 * 1048576;
            // mbbo is now 14 MB

            byte[] writeBuffer = new byte[1048576];
            for (int j = 0; j < writeBuffer.length; ++j) {
                writeBuffer[j] = (byte) random.nextInt(Byte.MAX_VALUE);
            }
            mbbo.put(writeBuffer);
            writeBytes += 1048576;
            // mbbo is now 15 MB

            mbbo.put(writeBuffer, 0, writeBuffer.length);
            writeBytes += 1048576;
            // mbbo is now 16 MB

            // set up 3 write buffers
            ByteBuffer wrappedWriteBuffer = ByteBuffer.wrap(writeBuffer);
            ByteBuffer allocatedWriteBuffer = ByteBuffer.allocate(1048576);
            allocatedWriteBuffer.put(writeBuffer);
            allocatedWriteBuffer.position(0);
            ByteBuffer allocatedDirectWriteBuffer = ByteBuffer
                    .allocateDirect(1048576);
            allocatedDirectWriteBuffer.put(writeBuffer);
            allocatedDirectWriteBuffer.position(0);

            // write the 3 buffers
            mbbo.put(wrappedWriteBuffer);
            writeBytes += 1048576;
            mbbo.put(allocatedWriteBuffer);
            writeBytes += 1048576;
            mbbo.put(allocatedDirectWriteBuffer);
            writeBytes += 1048576;
            // mbbo is now 19 MB

            // write to a dummy file so we can map its buffer
            // this gives an extra 1 MB in the write statistics
            File dummyFile = File.createTempFile("from", null);
            RandomAccessFile dummyRaf = new RandomAccessFile(dummyFile, "rw");
            ++openOperations;
            dummyRaf.write(writeBuffer);
            writeBytes += 1048576;

            dummyRaf.close();

            // this gives an extra 1 MB in the read statistics
            dummyRaf = new RandomAccessFile(dummyFile, "r");
            ++openOperations;

            mbbo.put(dummyRaf.getChannel().map(MapMode.READ_ONLY, 0, 1048576));
            writeBytes += 1048576;
            readBytes += 1048576;
            // mbbo is now 20 MB

            dummyRaf.close();

            try {
                mbbo.put(Byte.MAX_VALUE);
                assert (false);
            } catch (BufferOverflowException e) {
                // expected
            }

            fco.close();
            writeFile.close();
            assert (file.length() == 20 * 1048576);

            // Read

            RandomAccessFile readFile = new RandomAccessFile(file, "r");
            ++openOperations;

            FileChannel fci = readFile.getChannel();

            MappedByteBuffer mbbi = fci.map(MapMode.READ_ONLY, 0, 20 * 1048576);

            index = 0;

            for (int i = 0; i < 1048576; ++i) {
                assert (mbbi.get() == bytes[i]);
                index += 1;
                assert (mbbi.get(index) == bytes[i]);
                index += 1;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 2 MB

            for (int i = 0; i < 524288; ++i) {
                assert (mbbi.getChar() == chars[i]);
                index += 2;
                assert (mbbi.getChar(index) == chars[i]);
                index += 2;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 4 MB

            for (int i = 0; i < 131072; ++i) {
                assert (mbbi.getDouble() == doubles[i]);
                index += 8;
                assert (mbbi.getDouble(index) == doubles[i]);
                index += 8;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 6 MB

            for (int i = 0; i < 262144; ++i) {
                assert (mbbi.getFloat() == floats[i]);
                index += 4;
                assert (mbbi.getFloat(index) == floats[i]);
                index += 4;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 8 MB

            for (int i = 0; i < 262144; ++i) {
                assert (mbbi.getInt() == ints[i]);
                index += 4;
                assert (mbbi.getInt(index) == ints[i]);
                index += 4;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 10 MB

            for (int i = 0; i < 131072; ++i) {
                assert (mbbi.getLong() == longs[i]);
                index += 8;
                assert (mbbi.getLong(index) == longs[i]);
                index += 8;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 12 MB

            for (int i = 0; i < 524288; ++i) {
                assert (mbbi.getShort() == shorts[i]);
                index += 2;
                assert (mbbi.getShort(index) == shorts[i]);
                index += 2;
                mbbi.position(index);
            }
            readBytes += 2 * 1048576;
            // mbbi is now 14 MB

            byte[] readBuffer = new byte[1048576];
            mbbi.get(readBuffer);
            readBytes += 1048576;
            // mbbi is now 15 MB

            for (int j = 0; j < readBuffer.length; ++j) {
                assert (readBuffer[j] == writeBuffer[j]);
            }

            mbbi.get(readBuffer, 0, readBuffer.length);
            readBytes += 1048576;
            // mbbi is now 16 MB

            for (int j = 0; j < readBuffer.length; ++j) {
                assert (readBuffer[j] == writeBuffer[j]);
            }

            // set up 3 read buffers to read the last MB of input
            ByteBuffer wrappedReadBuffer = ByteBuffer.wrap(readBuffer);
            ByteBuffer allocatedReadBuffer = ByteBuffer.allocate(1048576);
            ByteBuffer allocatedDirectReadBuffer = ByteBuffer
                    .allocateDirect(1048576);

            int lastMBPosition = mbbi.capacity() - 1048576;
            // mbbi is now at 19 MB

            // read the 3 buffers

            // FIXME It is lucky that this works because of HeapByteBuffer's
            // put(ByteBuffer) implementation, as it invokes our instrumented
            // methods. If it were to do something fancy, we would miss these
            // two reads.
            mbbi.position(lastMBPosition);
            wrappedReadBuffer.put(mbbi);
            readBytes += 1048576;
            mbbi.position(lastMBPosition);
            allocatedReadBuffer.put(mbbi);
            readBytes += 1048576;

            // this is a DirectByteBuffer which we have already instrumented
            mbbi.position(lastMBPosition);
            allocatedDirectReadBuffer.put(mbbi);
            readBytes += 1048576;

            // this gives an extra 1 MB in the write statistics
            dummyRaf = new RandomAccessFile(dummyFile, "rw");
            ++openOperations;

            mbbi.position(lastMBPosition);
            dummyRaf.getChannel().map(MapMode.READ_WRITE, 0, 1048576).put(mbbi);
            writeBytes += 1048576;
            readBytes += 1048576;
            // mbbi is now 20 MB

            dummyRaf.close();

            try {
                mbbi.get();
                assert (false);
            } catch (BufferUnderflowException e) {
                // expected
            }

            fci.close();
            readFile.close();

            file.delete();
        }

        {
            // play around with Zip and Jar files, as apparently they behave
            // somewhat different
            File file = File.createTempFile("zip", null);

            // this is covered via our FileOutputStream instrumentation
            ZipOutputStream zos = new ZipOutputStream(
                    new BufferedOutputStream(new FileOutputStream(file)));
            ++openOperations;

            // compression ratio for random bytes seems to be around 87.7%, so
            // choose a little more than 1MB here such that we get around 1MB of
            // actual writes
            byte[] writeData = new byte[1196250];
            for (int i = 0; i < writeData.length; ++i) {
                writeData[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }

            zos.putNextEntry(new ZipEntry("ze"));
            zos.write(writeData);
            zos.closeEntry();

            zos.close();
            writeBytes += file.length();

            // this is covered via our FileInputStream instrumentation
            ZipInputStream zis = new ZipInputStream(
                    new BufferedInputStream(new FileInputStream(file)));
            ++openOperations;

            byte[] readData = new byte[writeData.length];
            ZipEntry ze = zis.getNextEntry();
            assert ("ze".equals(ze.getName()));

            int numRead = 0;
            while (readData.length - numRead > 0) {
                numRead += zis.read(readData, numRead,
                        readData.length - numRead);
            }
            assert (numRead == readData.length);

            for (int i = 0; i < readData.length; ++i) {
                assert (writeData[i] == readData[i]);
            }

            assert (zis.read() == -1);

            zis.close();
            readBytes += file.length();

            // ZipFile, on the other hand is different, because it uses caching
            // in the constructor.
            ZipFile zipFile = new ZipFile(file);
            readBytes += file.length();

            assert (zipFile.size() == 1);
            ze = zipFile.getEntry("ze");
            assert ("ze".equals(ze.getName()));

            // should not induce any more reads
            for (int i = 0; i < 3; ++i) {
                InputStream is = zipFile.getInputStream(ze);
                numRead = 0;
                while (readData.length - numRead > 0) {
                    numRead += is.read(readData, numRead,
                            readData.length - numRead);
                }
                assert (numRead == readData.length);

                for (int j = 0; j < readData.length; ++j) {
                    assert (writeData[j] == readData[j]);
                }
            }

            zipFile.close();
            file.delete();

            // repeat for Jar files
            file = File.createTempFile("jar", null);

            JarOutputStream jos = new JarOutputStream(
                    new BufferedOutputStream(new FileOutputStream(file)));
            ++openOperations;

            jos.putNextEntry(new ZipEntry("je"));
            jos.write(writeData);
            jos.closeEntry();

            jos.close();
            writeBytes += file.length();

            JarInputStream jis = new JarInputStream(
                    new BufferedInputStream(new FileInputStream(file)));
            ++openOperations;

            ZipEntry je = jis.getNextEntry();
            assert ("je".equals(je.getName()));

            numRead = 0;
            while (readData.length - numRead > 0) {
                numRead += jis.read(readData, numRead,
                        readData.length - numRead);
            }
            assert (numRead == readData.length);

            for (int i = 0; i < readData.length; ++i) {
                assert (writeData[i] == readData[i]);
            }

            assert (jis.read() == -1);

            jis.close();
            readBytes += file.length();

            JarFile jarFile = new JarFile(file);
            readBytes += file.length();

            assert (jarFile.size() == 1);
            je = jarFile.getEntry("je");
            assert ("je".equals(je.getName()));

            for (int i = 0; i < 3; ++i) {
                InputStream is = jarFile.getInputStream(je);
                numRead = 0;
                while (readData.length - numRead > 0) {
                    numRead += is.read(readData, numRead,
                            readData.length - numRead);
                }
                assert (numRead == readData.length);

                for (int j = 0; j < readData.length; ++j) {
                    assert (writeData[j] == readData[j]);
                }
            }

            jarFile.close();
            file.delete();
        }

        // shutdown the aggregator and read what it has written
        LiveOperationStatisticsAggregator aggregator = LiveOperationStatisticsAggregator.instance;

        if (!aggregator.isInitialized()) {
            // no instrumentation, we're done here
            return;
        }

        aggregator.shutdown();

        List<NavigableMap<Long, OperationStatistics>> aggregates = new ArrayList<NavigableMap<Long, OperationStatistics>>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregates.add(new ConcurrentSkipListMap<>());
        }

        // figure out the directory of log files
        File outputDirectory = new File(aggregator.getLogFilePrefix())
                .getParentFile();

        // for each category, read all log files
        for (OperationCategory category : OperationCategory.values()) {
            File[] categoryFiles = outputDirectory
                    .listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return name.contains(
                                    OperationSource.JVM.name().toLowerCase())
                                    && name.contains(
                                            category.name().toLowerCase())
                                    && name.endsWith("csv");
                        }
                    });

            // parse all files into OperationStatistics
            for (File file : categoryFiles) {
                BufferedReader reader = new BufferedReader(
                        new FileReader(file));

                // skip header
                String line = reader.readLine();
                while ((line = reader.readLine()) != null) {
                    // LiveOperationStatisticsAggregator prepends hostname, pid
                    // and key for each line
                    OperationStatistics operationStatistics;
                    switch (category) {
                    case OTHER:
                        operationStatistics = OperationStatistics.fromCsv(line,
                                aggregator.getOutputSeparator(), 3);
                        break;
                    case WRITE:
                        operationStatistics = DataOperationStatistics.fromCsv(
                                line, aggregator.getOutputSeparator(), 3);
                        break;
                    case READ:
                        operationStatistics = ReadDataOperationStatistics
                                .fromCsv(line, aggregator.getOutputSeparator(),
                                        3);
                        break;
                    default:
                        throw new IllegalArgumentException(category.name());
                    }

                    // JVM must be the only source, no SFS involved
                    assert (OperationSource.JVM
                            .equals(operationStatistics.getSource()));

                    // put the aggregates into the appropriate list/bin
                    aggregates.get(LiveOperationStatisticsAggregator
                            .getUniqueIndex(operationStatistics.getSource(),
                                    operationStatistics.getCategory()))
                            .put(operationStatistics.getTimeBin(),
                                    operationStatistics);
                }
                reader.close();
            }
        }

        // we opened the file a few times, however the JVM might open a lot
        // more files, especially during class loading, so no exact
        // estimation possible
        assertOperationCount(aggregates, OperationSource.JVM,
                OperationCategory.OTHER, openOperations);

        // Allow 8K slack for the JVM for writing, 160K for reading. This should
        // be fine as we always operate on 1 MB chunks of data, so if we truly
        // miss some operations, these tests should still fail. The slack is
        // mainly for reading Java classes which we instrument too, as well as
        // some internal lock file writing.
        assertOperationData(aggregates, OperationSource.JVM,
                OperationCategory.WRITE, writeBytes, writeBytes + 8 * 1024);
        assertOperationData(aggregates, OperationSource.JVM,
                OperationCategory.READ, readBytes, readBytes + 160 * 1024);

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
