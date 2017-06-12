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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
    // size granularity in bytes to use for tests
    private static final int BUFFER_SIZE = 1048576;
    private static final int BUFFER_SIZE_HALF = BUFFER_SIZE / 2;
    private static final int BUFFER_SIZE_QUARTER = BUFFER_SIZE / 4;
    private static final int BUFFER_SIZE_EIGHTH = BUFFER_SIZE / 8;
    private static final int BUFFER_SIZE_OVER_32768 = BUFFER_SIZE / 32768;

    // same random bytes for every run
    private static final Random RANDOM = new Random(0);

    // count operations and data
    private static int openOperations = 0;
    private static long readBytes = 0, writeBytes = 0;
    private static long jvmZipReadBytes = -1, zipReadBytes = -1;

    // whether to count mmap calls as well or not
    private static boolean traceMmap;

    public static void main(String[] args)
            throws IOException, InterruptedException, ExecutionException {
        // expect <test>,<true|false> as argument
        assert (args.length == 1);
        String[] arguments = args[0].split(",");
        assert (arguments.length == 2);
        String test = arguments[0];
        boolean useThreading = Boolean.parseBoolean(arguments[1]);

        int numProcessors = useThreading
                ? Runtime.getRuntime().availableProcessors() : 1;
        ExecutorService executor = Executors.newFixedThreadPool(numProcessors);

        traceMmap = Boolean
                .parseBoolean(System.getProperty("de.zib.sfs.traceMmap"));

        switch (test) {
        case "stream":
            runStreamTest(executor, numProcessors);
            break;
        case "random":
            runRandomTest(executor, numProcessors);
            break;
        case "channel":
            runChannelTest(executor, numProcessors);
            break;
        case "mapped":
            runMappedTest(executor, numProcessors);
            break;
        case "zip":
            runZipTest(executor, numProcessors);
            break;
        case "all":
            runStreamTest(executor, numProcessors);
            runRandomTest(executor, numProcessors);
            runChannelTest(executor, numProcessors);
            runMappedTest(executor, numProcessors);
            runZipTest(executor, numProcessors);
            break;
        case "none":
            return;
        default:
            assert (false) : test;
        }

        executor.shutdown();

        assertStatistics();
    }

    private static void runStreamTest(ExecutorService executor,
            int numProcessors) throws IOException {
        // TODO add threading

        File file = File.createTempFile("stream", null);
        file.deleteOnExit();

        // Write

        FileOutputStream fos = new FileOutputStream(file);
        ++openOperations;

        // use single byte writes
        byte[] writeBuffer = new byte[BUFFER_SIZE];
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            writeBuffer[i] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
            fos.write(writeBuffer[i]);
        }
        writeBytes += BUFFER_SIZE;

        // use simple array write
        fos.write(writeBuffer);
        writeBytes += BUFFER_SIZE;

        // use offset/length array write
        fos.write(writeBuffer, 0, writeBuffer.length);
        writeBytes += BUFFER_SIZE;

        fos.close();
        assert (file.length() == 3L * BUFFER_SIZE) : file.length() + " : "
                + 3L * BUFFER_SIZE;

        // Read

        FileInputStream fis = new FileInputStream(file);
        ++openOperations;

        // use single byte reads
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (fis.read() == writeBuffer[i]);
        }
        readBytes += BUFFER_SIZE;

        // use simple array read
        byte[] readBuffer = new byte[BUFFER_SIZE];
        int numRead = fis.read(readBuffer);
        readBytes += BUFFER_SIZE;
        assert (numRead == BUFFER_SIZE) : numRead + " : " + BUFFER_SIZE;
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (writeBuffer[i] == readBuffer[i]);
        }

        // use offset/length array read
        numRead = fis.read(readBuffer, 0, readBuffer.length);
        readBytes += BUFFER_SIZE;
        assert (numRead == BUFFER_SIZE) : numRead + " : " + BUFFER_SIZE;
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (writeBuffer[i] == readBuffer[i]);
        }

        numRead = fis.read();
        assert (numRead == -1);
        fis.close();

        file.delete();
    }

    private static void runRandomTest(ExecutorService executor,
            int numProcessors) throws IOException {
        assert (numProcessors == 1);

        File file = File.createTempFile("random", null);
        file.deleteOnExit();

        // Write

        RandomAccessFile writeFile = new RandomAccessFile(file, "rw");
        ++openOperations;

        byte[] writeBuffer = new byte[BUFFER_SIZE];
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            writeBuffer[i] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
            writeFile.write(writeBuffer[i]);
        }
        writeBytes += BUFFER_SIZE;

        writeFile.write(writeBuffer);
        writeBytes += BUFFER_SIZE;

        writeFile.write(writeBuffer, 0, writeBuffer.length);
        writeBytes += BUFFER_SIZE;

        boolean[] bools = new boolean[BUFFER_SIZE];
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            bools[i] = RANDOM.nextBoolean();
            writeFile.writeBoolean(bools[i]);
        }
        writeBytes += BUFFER_SIZE;

        byte[] bytes = new byte[BUFFER_SIZE];
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            bytes[i] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
            writeFile.writeByte(bytes[i]);
        }
        writeBytes += BUFFER_SIZE;

        String string = new String(bytes);
        writeFile.writeBytes(string);
        writeBytes += BUFFER_SIZE;

        char[] chars = new char[BUFFER_SIZE_HALF];
        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            chars[i] = (char) RANDOM.nextInt(Character.MAX_VALUE);
            writeFile.writeChar(chars[i]);
        }
        writeBytes += BUFFER_SIZE;

        double[] doubles = new double[BUFFER_SIZE_EIGHTH];
        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            doubles[i] = RANDOM.nextDouble();
            writeFile.writeDouble(doubles[i]);
        }
        writeBytes += BUFFER_SIZE;

        float[] floats = new float[BUFFER_SIZE_QUARTER];
        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            floats[i] = RANDOM.nextFloat();
            writeFile.writeFloat(floats[i]);
        }
        writeBytes += BUFFER_SIZE;

        int[] ints = new int[BUFFER_SIZE_QUARTER];
        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            ints[i] = RANDOM.nextInt();
            writeFile.writeInt(ints[i]);
        }
        writeBytes += BUFFER_SIZE;

        long[] longs = new long[BUFFER_SIZE_EIGHTH];
        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            longs[i] = RANDOM.nextLong();
            writeFile.writeLong(longs[i]);
        }
        writeBytes += BUFFER_SIZE;

        short[] shorts = new short[BUFFER_SIZE_HALF];
        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            shorts[i] = (short) RANDOM.nextInt(Short.MAX_VALUE);
            writeFile.writeShort(shorts[i]);
        }
        writeBytes += BUFFER_SIZE;

        writeFile.writeChars(string.substring(0, BUFFER_SIZE_HALF));
        writeBytes += BUFFER_SIZE;

        // 64K restriction on UTF-8 string length
        // so use 32 * 32K strings
        int utf8StringLength = 0;
        for (int i = 0; i < BUFFER_SIZE_OVER_32768; ++i) {
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
        writeBytes += BUFFER_SIZE;

        writeFile.close();
        // 2 bytes extra per UTF write
        long expected = 13L * BUFFER_SIZE + utf8StringLength
                + 2L * BUFFER_SIZE_OVER_32768;
        assert (file.length() == expected) : file.length() + " : " + expected;

        // Read

        RandomAccessFile readFile = new RandomAccessFile(file, "r");
        ++openOperations;

        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (readFile.read() == writeBuffer[i]);
        }
        readBytes += BUFFER_SIZE;

        byte[] readBuffer = new byte[BUFFER_SIZE];
        int numRead = readFile.read(readBuffer);
        readBytes += BUFFER_SIZE;
        assert (numRead == BUFFER_SIZE) : numRead + " : " + BUFFER_SIZE;
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (writeBuffer[i] == readBuffer[i]);
        }

        numRead = readFile.read(readBuffer, 0, readBuffer.length);
        readBytes += BUFFER_SIZE;
        assert (numRead == BUFFER_SIZE) : numRead + " : " + BUFFER_SIZE;
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (writeBuffer[i] == readBuffer[i]);
        }

        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (readFile.readBoolean() == bools[i]);
        }
        readBytes += BUFFER_SIZE;

        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (readFile.readByte() == bytes[i]);
        }
        readBytes += BUFFER_SIZE;

        readFile.readFully(readBuffer);
        readBytes += BUFFER_SIZE;
        assert (new String(readBuffer).equals(string));

        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            assert (readFile.readChar() == chars[i]);
        }
        readBytes += BUFFER_SIZE;

        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            assert (readFile.readDouble() == doubles[i]);
        }
        readBytes += BUFFER_SIZE;

        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            assert (readFile.readFloat() == floats[i]);
        }
        readBytes += BUFFER_SIZE;

        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            assert (readFile.readInt() == ints[i]);
        }
        readBytes += BUFFER_SIZE;

        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            assert (readFile.readLong() == longs[i]);
        }
        readBytes += BUFFER_SIZE;

        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            assert (readFile.readShort() == shorts[i]);
        }
        readBytes += BUFFER_SIZE;

        readFile.readFully(readBuffer, 0, readBuffer.length);
        readBytes += BUFFER_SIZE;
        char[] characters = new char[BUFFER_SIZE_HALF];
        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            characters[i] = (char) ((readBuffer[2 * i] << 8)
                    | readBuffer[2 * i + 1]);
        }
        assert (new String(characters)
                .equals(string.substring(0, BUFFER_SIZE_HALF)));

        for (int i = 0; i < BUFFER_SIZE_OVER_32768; ++i) {
            assert (readFile.readUTF().equals(
                    string.substring(i * 32 * 1024, (i + 1) * 32 * 1024)));
        }
        readBytes += BUFFER_SIZE;

        numRead = readFile.read();
        assert (numRead == -1);
        readFile.close();

        file.delete();
    }

    private static void runChannelTest(ExecutorService executor,
            int numProcessors)
            throws IOException, InterruptedException, ExecutionException {
        File file = File.createTempFile("channel", null);
        file.deleteOnExit();

        // Write

        FileOutputStream fos = new FileOutputStream(file);
        ++openOperations;

        FileChannel fco = fos.getChannel();

        final byte[] writeBuffer = new byte[BUFFER_SIZE];
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            writeBuffer[i] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
        }

        // set up 3 write buffers
        final ByteBuffer wrappedWriteBuffer = ByteBuffer.wrap(writeBuffer);
        ByteBuffer allocatedWriteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        allocatedWriteBuffer.put(writeBuffer);
        allocatedWriteBuffer.position(0);
        ByteBuffer allocatedDirectWriteBuffer = ByteBuffer
                .allocateDirect(BUFFER_SIZE);
        allocatedDirectWriteBuffer.put(writeBuffer);
        allocatedDirectWriteBuffer.position(0);

        // write all 3 buffers
        long numWritten = fco.write(wrappedWriteBuffer);
        wrappedWriteBuffer.position(0);
        writeBytes += BUFFER_SIZE;
        numWritten += fco.write(allocatedWriteBuffer);
        allocatedWriteBuffer.position(0);
        writeBytes += BUFFER_SIZE;
        numWritten += fco.write(allocatedDirectWriteBuffer);
        allocatedDirectWriteBuffer.position(0);
        writeBytes += BUFFER_SIZE;
        // fco is now 3 MB

        // repeat with duplicates
        numWritten += fco.write(wrappedWriteBuffer.duplicate());
        wrappedWriteBuffer.position(0);
        writeBytes += BUFFER_SIZE;
        numWritten += fco.write(allocatedWriteBuffer.duplicate());
        allocatedWriteBuffer.position(0);
        writeBytes += BUFFER_SIZE;
        numWritten += fco.write(allocatedDirectWriteBuffer.duplicate());
        allocatedDirectWriteBuffer.position(0);
        writeBytes += BUFFER_SIZE;
        // fco is now 3 MB

        // write all 3 buffers using offsets, possibly concurrently
        List<Future<Long>> numsWritten = new ArrayList<Future<Long>>();
        long currentFcoPosition = fco.position();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFcoPosition + i * 3L * BUFFER_SIZE;
            numsWritten.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long numWritten = fco.write(wrappedWriteBuffer.duplicate(),
                            offset);
                    numWritten += fco.write(allocatedWriteBuffer.duplicate(),
                            offset + BUFFER_SIZE);
                    numWritten += fco.write(
                            allocatedDirectWriteBuffer.duplicate(),
                            offset + 2L * BUFFER_SIZE);
                    return numWritten;
                }
            }));
        }
        for (Future<Long> nw : numsWritten) {
            numWritten += nw.get();
        }
        fco.position(fco.position() + numProcessors * 3L * BUFFER_SIZE);
        writeBytes += numProcessors * 3L * BUFFER_SIZE;
        // fco is now 6 MB (for numProcessors == 1)

        // write all 3 buffers using an array of them
        numWritten += fco.write(new ByteBuffer[] { wrappedWriteBuffer,
                allocatedWriteBuffer, allocatedDirectWriteBuffer });
        writeBytes += 3L * BUFFER_SIZE;
        // fco is now 9 MB

        // write to a dummy file so we can map its buffer
        // this gives an extra 1 MB in the write statistics
        File dummyFile = File.createTempFile("dummy", null);
        dummyFile.deleteOnExit();
        RandomAccessFile dummyRaf = new RandomAccessFile(dummyFile, "rw");
        ++openOperations;
        for (int i = 0; i < numProcessors; ++i) {
            dummyRaf.write(writeBuffer);
            writeBytes += BUFFER_SIZE;
        }

        dummyRaf.close();

        dummyRaf = new RandomAccessFile(dummyFile, "r");
        ++openOperations;
        final FileChannel readDummyRafChannel = dummyRaf.getChannel();
        final MappedByteBuffer readMappedByteBuffer = readDummyRafChannel
                .map(MapMode.READ_ONLY, 0, BUFFER_SIZE);

        // use regular write
        numWritten += fco.write(readMappedByteBuffer);
        writeBytes += BUFFER_SIZE;
        readBytes += traceMmap ? BUFFER_SIZE : 0L;
        readMappedByteBuffer.position(0);
        // fco is now 10 MB

        // use regular write and duplicate()
        numWritten += fco.write(readMappedByteBuffer.duplicate());
        writeBytes += BUFFER_SIZE;
        readBytes += traceMmap ? BUFFER_SIZE : 0L;

        // use write with offset
        numsWritten.clear();
        currentFcoPosition = fco.position();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFcoPosition + 1L * i * BUFFER_SIZE;
            numsWritten.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return (long) fco.write(readMappedByteBuffer.duplicate(),
                            offset);
                }
            }));
        }
        for (Future<Long> nw : numsWritten) {
            numWritten += nw.get();
        }
        fco.position(fco.position() + 1L * numProcessors * BUFFER_SIZE);
        writeBytes += 1L * numProcessors * BUFFER_SIZE;
        readBytes += traceMmap ? 1L * numProcessors * BUFFER_SIZE : 0L;
        // fco is now 11 MB (for numProcessors == 1)

        // use array write, combined with asReadOnlyBuffer and slice
        numWritten += fco.write(
                new ByteBuffer[] { readMappedByteBuffer.asReadOnlyBuffer(),
                        readMappedByteBuffer.slice(), readMappedByteBuffer });
        writeBytes += 3L * BUFFER_SIZE;
        readBytes += traceMmap ? 3L * BUFFER_SIZE : 0L;
        readMappedByteBuffer.position(0);
        // fco is now 12 MB

        // use transfer from file, incurring another 1 MB of reads
        numsWritten.clear();
        currentFcoPosition = fco.position();
        readDummyRafChannel.position(0);
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFcoPosition + 1L * i * BUFFER_SIZE;
            numsWritten.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long remaining = BUFFER_SIZE;
                    while (remaining > 0) {
                        remaining -= fco.transferFrom(readDummyRafChannel,
                                offset + BUFFER_SIZE - remaining, remaining);
                    }
                    return (long) BUFFER_SIZE;
                }
            }));
        }
        for (Future<Long> nw : numsWritten) {
            numWritten += nw.get();
        }
        fco.position(fco.position() + 1L * numProcessors * BUFFER_SIZE);
        writeBytes += 1L * numProcessors * BUFFER_SIZE;
        readBytes += traceMmap ? 1L * numProcessors * BUFFER_SIZE : 0L;
        // fco is now 13 MB (for numProcessors == 1)

        dummyRaf.close();

        // use transfer from arbitrary source
        currentFcoPosition = fco.position();
        numsWritten.clear();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFcoPosition + 1L * i * BUFFER_SIZE;
            numsWritten.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long remaining = BUFFER_SIZE;
                    while (remaining > 0) {
                        remaining -= fco
                                .transferFrom(new ReadableByteChannel() {
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
                                    public int read(ByteBuffer dst)
                                            throws IOException {
                                        // produce random bytes from this
                                        // channel
                                        byte[] src = new byte[dst.remaining()];
                                        for (int i = 0; i < src.length; ++i) {
                                            src[i] = (byte) InstrumentationTest.RANDOM
                                                    .nextInt(Byte.MAX_VALUE);
                                        }
                                        dst.put(src, 0, src.length);
                                        return src.length;
                                    }
                                }, offset + BUFFER_SIZE - remaining, remaining);
                    }
                    return (long) BUFFER_SIZE;
                }
            }));
        }
        for (Future<Long> nw : numsWritten) {
            numWritten += nw.get();
        }
        fco.position(fco.position() + 1L * numProcessors * BUFFER_SIZE);
        writeBytes += 1L * numProcessors * BUFFER_SIZE;
        // fco is now 14 MB (for numProcessors == 1)

        fco.close();
        fos.close();
        long expected = 14L * BUFFER_SIZE + 6L * numProcessors * BUFFER_SIZE;
        assert (numWritten == expected) : numWritten + " : " + expected;
        assert (file.length() == expected) : file.length() + " : " + expected;

        // Read

        FileInputStream fis = new FileInputStream(file);
        ++openOperations;

        FileChannel fci = fis.getChannel();
        assert (fci.size() == expected) : fci.size() + " : " + expected;
        assert (fci.position() == 0) : fci.position() + " : " + 0;

        byte[] readBuffer = new byte[BUFFER_SIZE];

        // set up 3 read buffers
        ByteBuffer wrappedReadBuffer = ByteBuffer.wrap(readBuffer);
        ByteBuffer allocatedReadBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        ByteBuffer allocatedDirectReadBuffer = ByteBuffer
                .allocateDirect(BUFFER_SIZE);

        // read all 3 buffers
        long numRead = fci.read(wrappedReadBuffer);
        wrappedReadBuffer.position(0);
        readBytes += BUFFER_SIZE;
        assert (numRead == BUFFER_SIZE) : numRead + " : " + BUFFER_SIZE;
        assert (fci.position() == BUFFER_SIZE) : fci.position() + " : "
                + BUFFER_SIZE;
        numRead += fci.read(allocatedReadBuffer);
        allocatedReadBuffer.position(0);
        readBytes += BUFFER_SIZE;
        assert (numRead == 2L * BUFFER_SIZE) : numRead + " : "
                + 2L * BUFFER_SIZE;
        assert (fci.position() == 2L * BUFFER_SIZE) : fci.position() + " : "
                + 2L * BUFFER_SIZE;
        numRead += fci.read(allocatedDirectReadBuffer);
        allocatedDirectReadBuffer.position(0);
        readBytes += BUFFER_SIZE;
        assert (numRead == 3L * BUFFER_SIZE) : numRead + " : "
                + 3L * BUFFER_SIZE;
        assert (fci.position() == 3L * BUFFER_SIZE) : fci.position() + " : "
                + 3L * BUFFER_SIZE;
        // fci is now 3 MB

        // repeat with duplicates
        numRead += fci.read(wrappedReadBuffer.duplicate());
        wrappedReadBuffer.position(0);
        readBytes += BUFFER_SIZE;
        assert (numRead == 4L * BUFFER_SIZE) : numRead + " : "
                + 4L * BUFFER_SIZE;
        assert (fci.position() == 4L * BUFFER_SIZE) : fci.position() + " : "
                + 4L * BUFFER_SIZE;
        numRead += fci.read(allocatedReadBuffer.duplicate());
        allocatedReadBuffer.position(0);
        readBytes += BUFFER_SIZE;
        assert (numRead == 5L * BUFFER_SIZE) : numRead + " : "
                + 5L * BUFFER_SIZE;
        assert (fci.position() == 5L * BUFFER_SIZE) : fci.position() + " : "
                + 5L * BUFFER_SIZE;
        numRead += fci.read(allocatedDirectReadBuffer.duplicate());
        allocatedDirectReadBuffer.position(0);
        readBytes += BUFFER_SIZE;
        assert (numRead == 6L * BUFFER_SIZE) : numRead + " : "
                + 6L * BUFFER_SIZE;
        assert (fci.position() == 6L * BUFFER_SIZE) : fci.position() + " : "
                + 6L * BUFFER_SIZE;
        // fci is now 6 MB

        // read all 3 buffers using offsets
        List<Future<Long>> numsRead = new ArrayList<Future<Long>>();
        long currentFciPosition = fci.position();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFciPosition + i * 3L * BUFFER_SIZE;
            numsRead.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long numRead = fci.read(wrappedReadBuffer.duplicate(),
                            offset);
                    numRead += fci.read(allocatedReadBuffer.duplicate(),
                            offset + BUFFER_SIZE);
                    numRead += fci.read(allocatedDirectReadBuffer.duplicate(),
                            offset + 2L * BUFFER_SIZE);
                    return numRead;
                }
            }));
        }
        for (Future<Long> nr : numsRead) {
            numRead += nr.get();
        }
        fci.position(fci.position() + numProcessors * 3L * BUFFER_SIZE);
        readBytes += numProcessors * 3L * BUFFER_SIZE;
        expected = 6L * BUFFER_SIZE + 3L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;
        // fci is now 9 MB (for numProcessors == 1)

        // read all 3 buffers using an array of them
        wrappedReadBuffer.position(0);
        allocatedReadBuffer.position(0);
        allocatedDirectReadBuffer.position(0);
        numRead += fci.read(new ByteBuffer[] { wrappedReadBuffer,
                allocatedReadBuffer, allocatedDirectReadBuffer });
        readBytes += 3L * BUFFER_SIZE;
        expected = 9L * BUFFER_SIZE + 3L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;
        // fci is now 12 MB

        // verify contents
        wrappedWriteBuffer.position(0);
        wrappedReadBuffer.position(0);
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (wrappedWriteBuffer.get() == wrappedReadBuffer.get());
        }
        allocatedWriteBuffer.position(0);
        allocatedReadBuffer.position(0);
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (allocatedWriteBuffer.get() == allocatedReadBuffer.get());
        }
        allocatedDirectWriteBuffer.position(0);
        allocatedDirectReadBuffer.position(0);
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (allocatedDirectWriteBuffer
                    .get() == allocatedDirectReadBuffer.get());
        }

        // read content back to file
        dummyRaf = new RandomAccessFile(dummyFile, "rw");
        ++openOperations;
        final FileChannel writeDummyRafChannel = dummyRaf.getChannel();
        MappedByteBuffer writeMappedByteBuffer = writeDummyRafChannel
                .map(MapMode.READ_WRITE, 0, BUFFER_SIZE);

        // use regular read
        numRead += fci.read(writeMappedByteBuffer);
        writeBytes += traceMmap ? BUFFER_SIZE : 0L;
        readBytes += BUFFER_SIZE;
        writeMappedByteBuffer.position(0);
        expected = 10L * BUFFER_SIZE + 3L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;
        // fci is now 13 MB

        // use regular read with duplicate
        numRead += fci.read(writeMappedByteBuffer.duplicate());
        writeBytes += traceMmap ? BUFFER_SIZE : 0L;
        readBytes += BUFFER_SIZE;
        expected = 11L * BUFFER_SIZE + 3L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;
        // fci is now at 14 MB
/*
        // use read with offset
        numsRead.clear();
        currentFciPosition = fci.position();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFciPosition + 1L * i * BUFFER_SIZE;
            numsRead.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    return (long) fci.read(writeMappedByteBuffer.duplicate(),
                            offset);
                }
            }));
        }
        for (Future<Long> nr : numsRead) {
            numRead += nr.get();
        }
        fci.position(fci.position() + 1L * numProcessors * BUFFER_SIZE);
        writeBytes += traceMmap ? 1L * numProcessors * BUFFER_SIZE : 0L;
        readBytes += 1L * numProcessors * BUFFER_SIZE;
        expected = 11L * BUFFER_SIZE + 4L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;
        // fci is now 15 MB (for numProcessors == 1)

        // use array read, combined with slice
        numRead += fci.read(new ByteBuffer[] { writeMappedByteBuffer.slice(),
                writeMappedByteBuffer.slice(), writeMappedByteBuffer });
        writeBytes += traceMmap ? 3L * BUFFER_SIZE : 0L;
        readBytes += 3L * BUFFER_SIZE;
        writeMappedByteBuffer.position(0);
        // fci is now 18 MB
        expected = 14L * BUFFER_SIZE + 4L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;

        // use transfer to file
        numsRead.clear();
        currentFciPosition = fci.position();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFciPosition + 1L * i * BUFFER_SIZE;
            numsRead.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long remaining = BUFFER_SIZE;
                    while (remaining > 0) {
                        remaining -= fci.transferTo(
                                offset + BUFFER_SIZE - remaining, remaining,
                                writeDummyRafChannel);
                    }
                    return (long) BUFFER_SIZE;
                }
            }));
        }
        for (Future<Long> nr : numsRead) {
            numRead += nr.get();
        }
        fci.position(fci.position() + 1L * numProcessors * BUFFER_SIZE);
        writeBytes += 1L * numProcessors * BUFFER_SIZE;
        readBytes += traceMmap ? 1L * numProcessors * BUFFER_SIZE : 0L;

        // fci is now 19 MB
        expected = 14L * BUFFER_SIZE + 5L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;

        dummyRaf.close();

        numsRead.clear();
        currentFciPosition = fci.position();
        for (int i = 0; i < numProcessors; ++i) {
            final long offset = currentFciPosition + 1L * i * BUFFER_SIZE;
            numsRead.add(executor.submit(new Callable<Long>() {
                @Override
                public Long call() throws Exception {
                    long remaining = BUFFER_SIZE;
                    while (remaining > 0) {
                        remaining -= fci.transferTo(
                                offset + BUFFER_SIZE - remaining, remaining,
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
                                    public int write(ByteBuffer src)
                                            throws IOException {
                                        // discard everything
                                        byte[] dst = new byte[src.remaining()];
                                        src.get(dst, 0, dst.length);
                                        return dst.length;
                                    }
                                });
                    }
                    return (long) BUFFER_SIZE;
                }
            }));
        }
        for (Future<Long> nr : numsRead) {
            numRead += nr.get();
        }
        fci.position(fci.position() + 1L * numProcessors * BUFFER_SIZE);
        readBytes += 1L * numProcessors * BUFFER_SIZE;
        // fci is now 20 MB
        expected = 14L * BUFFER_SIZE + 6L * numProcessors * BUFFER_SIZE;
        assert (numRead == expected) : numRead + " : " + expected;
        assert (fci.position() == expected) : fci.position() + " : " + expected;

        wrappedReadBuffer.position(0);
        numRead = fci.read(wrappedReadBuffer);
        assert (numRead == -1);
        fci.close();
        fis.close();
*/
        file.delete();
    }

    private static void runMappedTest(ExecutorService executor,
            int numProcessors) throws IOException {
        // TODO add threading

        File file = File.createTempFile("mapped", null);
        file.deleteOnExit();

        // Write

        RandomAccessFile writeFile = new RandomAccessFile(file, "rw");
        ++openOperations;

        FileChannel fco = writeFile.getChannel();

        MappedByteBuffer mbbo = fco.map(MapMode.READ_WRITE, 0,
                20L * BUFFER_SIZE);

        int index = 0;

        byte[] bytes = new byte[BUFFER_SIZE];
        for (int i = 0; i < BUFFER_SIZE; ++i) {
            bytes[i] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
            mbbo.put(bytes[i]);
            index += 1;
            mbbo.put(index, bytes[i]);
            index += 1;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 2 MB

        char[] chars = new char[BUFFER_SIZE_HALF];
        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            chars[i] = (char) RANDOM.nextInt(Character.MAX_VALUE);
            mbbo.putChar(chars[i]);
            index += 2;
            mbbo.putChar(index, chars[i]);
            index += 2;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 4 MB

        double[] doubles = new double[BUFFER_SIZE_EIGHTH];
        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            doubles[i] = RANDOM.nextDouble();
            mbbo.putDouble(doubles[i]);
            index += 8;
            mbbo.putDouble(index, doubles[i]);
            index += 8;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 6 MB

        float[] floats = new float[BUFFER_SIZE_QUARTER];
        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            floats[i] = RANDOM.nextFloat();
            mbbo.putFloat(floats[i]);
            index += 4;
            mbbo.putFloat(index, floats[i]);
            index += 4;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 8 MB

        int[] ints = new int[BUFFER_SIZE_QUARTER];
        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            ints[i] = RANDOM.nextInt();
            mbbo.putInt(ints[i]);
            index += 4;
            mbbo.putInt(index, ints[i]);
            index += 4;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 10 MB

        long[] longs = new long[BUFFER_SIZE_EIGHTH];
        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            longs[i] = RANDOM.nextLong();
            mbbo.putLong(longs[i]);
            index += 8;
            mbbo.putLong(index, longs[i]);
            index += 8;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 12 MB

        short[] shorts = new short[BUFFER_SIZE_HALF];
        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            shorts[i] = (short) RANDOM.nextInt(Short.MAX_VALUE);
            mbbo.putShort(shorts[i]);
            index += 2;
            mbbo.putShort(index, shorts[i]);
            index += 2;
            mbbo.position(index);
        }
        writeBytes += 2L * BUFFER_SIZE;
        // mbbo is now 14 MB

        byte[] writeBuffer = new byte[BUFFER_SIZE];
        for (int j = 0; j < writeBuffer.length; ++j) {
            writeBuffer[j] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
        }
        mbbo.put(writeBuffer);
        writeBytes += BUFFER_SIZE;
        // mbbo is now 15 MB

        mbbo.put(writeBuffer, 0, writeBuffer.length);
        writeBytes += BUFFER_SIZE;
        // mbbo is now 16 MB

        // set up 3 write buffers
        ByteBuffer wrappedWriteBuffer = ByteBuffer.wrap(writeBuffer);
        ByteBuffer allocatedWriteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        allocatedWriteBuffer.put(writeBuffer);
        allocatedWriteBuffer.position(0);
        ByteBuffer allocatedDirectWriteBuffer = ByteBuffer
                .allocateDirect(BUFFER_SIZE);
        allocatedDirectWriteBuffer.put(writeBuffer);
        allocatedDirectWriteBuffer.position(0);

        // write the 3 buffers
        mbbo.put(wrappedWriteBuffer);
        writeBytes += BUFFER_SIZE;
        mbbo.put(allocatedWriteBuffer);
        writeBytes += BUFFER_SIZE;
        mbbo.put(allocatedDirectWriteBuffer);
        writeBytes += BUFFER_SIZE;
        // mbbo is now 19 MB

        // write to a dummy file so we can map its buffer
        // this gives an extra 1 MB in the write statistics
        File dummyFile = File.createTempFile("from", null);
        dummyFile.deleteOnExit();
        RandomAccessFile dummyRaf = new RandomAccessFile(dummyFile, "rw");
        ++openOperations;
        dummyRaf.write(writeBuffer);
        writeBytes += BUFFER_SIZE;

        dummyRaf.close();

        // this gives an extra 1 MB in the read statistics
        dummyRaf = new RandomAccessFile(dummyFile, "r");
        ++openOperations;

        mbbo.put(dummyRaf.getChannel().map(MapMode.READ_ONLY, 0, BUFFER_SIZE));
        writeBytes += BUFFER_SIZE;
        readBytes += BUFFER_SIZE;
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
        assert (file.length() == 20L * BUFFER_SIZE) : file.length() + " : "
                + 20L * BUFFER_SIZE;

        // Read

        RandomAccessFile readFile = new RandomAccessFile(file, "r");
        ++openOperations;

        FileChannel fci = readFile.getChannel();

        MappedByteBuffer mbbi = fci.map(MapMode.READ_ONLY, 0,
                20L * BUFFER_SIZE);

        index = 0;

        for (int i = 0; i < BUFFER_SIZE; ++i) {
            assert (mbbi.get() == bytes[i]);
            index += 1;
            assert (mbbi.get(index) == bytes[i]);
            index += 1;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 2 MB

        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            assert (mbbi.getChar() == chars[i]);
            index += 2;
            assert (mbbi.getChar(index) == chars[i]);
            index += 2;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 4 MB

        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            assert (mbbi.getDouble() == doubles[i]);
            index += 8;
            assert (mbbi.getDouble(index) == doubles[i]);
            index += 8;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 6 MB

        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            assert (mbbi.getFloat() == floats[i]);
            index += 4;
            assert (mbbi.getFloat(index) == floats[i]);
            index += 4;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 8 MB

        for (int i = 0; i < BUFFER_SIZE_QUARTER; ++i) {
            assert (mbbi.getInt() == ints[i]);
            index += 4;
            assert (mbbi.getInt(index) == ints[i]);
            index += 4;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 10 MB

        for (int i = 0; i < BUFFER_SIZE_EIGHTH; ++i) {
            assert (mbbi.getLong() == longs[i]);
            index += 8;
            assert (mbbi.getLong(index) == longs[i]);
            index += 8;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 12 MB

        for (int i = 0; i < BUFFER_SIZE_HALF; ++i) {
            assert (mbbi.getShort() == shorts[i]);
            index += 2;
            assert (mbbi.getShort(index) == shorts[i]);
            index += 2;
            mbbi.position(index);
        }
        readBytes += 2L * BUFFER_SIZE;
        // mbbi is now 14 MB

        byte[] readBuffer = new byte[BUFFER_SIZE];
        mbbi.get(readBuffer);
        readBytes += BUFFER_SIZE;
        // mbbi is now 15 MB

        for (int j = 0; j < readBuffer.length; ++j) {
            assert (readBuffer[j] == writeBuffer[j]);
        }

        mbbi.get(readBuffer, 0, readBuffer.length);
        readBytes += BUFFER_SIZE;
        // mbbi is now 16 MB

        for (int j = 0; j < readBuffer.length; ++j) {
            assert (readBuffer[j] == writeBuffer[j]);
        }

        // set up 3 read buffers to read the last MB of input
        ByteBuffer wrappedReadBuffer = ByteBuffer.wrap(readBuffer);
        ByteBuffer allocatedReadBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        ByteBuffer allocatedDirectReadBuffer = ByteBuffer
                .allocateDirect(BUFFER_SIZE);

        int lastMBPosition = mbbi.capacity() - BUFFER_SIZE;
        // mbbi is now at 19 MB

        // read the 3 buffers

        // FIXME It is lucky that this works because of HeapByteBuffer's
        // put(ByteBuffer) implementation, as it invokes our instrumented
        // methods. If it were to do something fancy, we would miss these
        // two reads.
        mbbi.position(lastMBPosition);
        wrappedReadBuffer.put(mbbi);
        readBytes += BUFFER_SIZE;
        mbbi.position(lastMBPosition);
        allocatedReadBuffer.put(mbbi);
        readBytes += BUFFER_SIZE;

        // this is a DirectByteBuffer which we have already instrumented
        mbbi.position(lastMBPosition);
        allocatedDirectReadBuffer.put(mbbi);
        readBytes += BUFFER_SIZE;

        // this gives an extra 1 MB in the write statistics
        dummyRaf = new RandomAccessFile(dummyFile, "rw");
        ++openOperations;

        mbbi.position(lastMBPosition);
        dummyRaf.getChannel().map(MapMode.READ_WRITE, 0, BUFFER_SIZE).put(mbbi);
        writeBytes += BUFFER_SIZE;
        readBytes += BUFFER_SIZE;
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

    private static void runZipTest(ExecutorService executor, int numProcessors)
            throws IOException {
        assert (numProcessors == 1);

        // play around with Zip and Jar files, as apparently they behave
        // somewhat different
        File file = File.createTempFile("zip", null);
        file.deleteOnExit();

        // this is covered via our FileOutputStream instrumentation
        ZipOutputStream zos = new ZipOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)));
        ++openOperations;

        // compression ratio for random bytes seems to be around 87.7%, so
        // choose a little more than 1MB here such that we get around 1MB of
        // actual writes
        byte[] writeData = new byte[(int) (1.14 * BUFFER_SIZE)];
        for (int i = 0; i < writeData.length; ++i) {
            writeData[i] = (byte) RANDOM.nextInt(Byte.MAX_VALUE);
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
            numRead += zis.read(readData, numRead, readData.length - numRead);
        }
        assert (numRead == readData.length) : numRead + " : " + readData.length;

        for (int i = 0; i < readData.length; ++i) {
            assert (writeData[i] == readData[i]);
        }

        assert (zis.read() == -1);

        zis.close();
        readBytes += file.length();

        // Add what has been loaded by the JVM until now as expected data.
        // This is because we have not used ZipFiles yet, however when the
        // JVM loads Jars, our ZipFileCallback was invoked already. So as to
        // manage our expectations for this test, we keep track of what was
        // loaded by the JVM until now and assume there won't be much more
        // afterwards.
        jvmZipReadBytes = ZipFileCallback.getTotalData();

        // ZipFile, on the other hand is different, because it uses caching
        // in the constructor.
        ZipFile zipFile = new ZipFile(file);
        zipReadBytes = file.length();

        assert (zipFile.size() == 1);
        ze = zipFile.getEntry("ze");
        assert ("ze".equals(ze.getName()));

        // should not induce any reads
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

        // opening the same ZIP should hit the cache
        new ZipFile(file).close();

        zipFile.close();

        // expect reads again
        new ZipFile(file).close();
        zipReadBytes += file.length();

        file.delete();

        // repeat for Jar files
        file = File.createTempFile("jar", null);
        file.deleteOnExit();

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
            numRead += jis.read(readData, numRead, readData.length - numRead);
        }
        assert (numRead == readData.length) : numRead + " : " + readData.length;

        for (int i = 0; i < readData.length; ++i) {
            assert (writeData[i] == readData[i]);
        }

        assert (jis.read() == -1);

        jis.close();
        readBytes += file.length();

        JarFile jarFile = new JarFile(file);
        zipReadBytes += file.length();

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
            assert (numRead == readData.length) : numRead + " : "
                    + readData.length;

            for (int j = 0; j < readData.length; ++j) {
                assert (writeData[j] == readData[j]);
            }
        }

        new JarFile(file).close();
        jarFile.close();

        new JarFile(file).close();
        zipReadBytes += file.length();

        file.delete();
    }

    private static void assertStatistics() throws IOException {
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
                    OperationStatistics operationStatistics = null;
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
                    case ZIP:
                        operationStatistics = ReadDataOperationStatistics
                                .fromCsv(line, aggregator.getOutputSeparator(),
                                        3);
                        break;
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

                // remove statistics file at the end to avoid counting it twice
                // in future test runs
                file.delete();
            }
        }

        // we opened the file a few times, however the JVM might open a lot
        // more files, especially during class loading, so no exact
        // estimation possible
        assertOperationCount(aggregates, OperationSource.JVM,
                OperationCategory.OTHER, openOperations);

        // Allow 64K slack for the JVM for writing, 176K for reading. This
        // should be fine as we always operate on 1 MB chunks of data, so if we
        // truly miss some operations, these tests should still fail. The slack
        // is mainly for reading Java classes which we instrument too, as well
        // as some internal lock file writing.
        assertOperationData(aggregates, OperationSource.JVM,
                OperationCategory.WRITE, writeBytes, writeBytes + 64 * 1024);
        System.err.println("Wrote " + writeBytes + " bytes");
        assertOperationData(aggregates, OperationSource.JVM,
                OperationCategory.READ, readBytes, readBytes + 176 * 1024);
        System.err.println("Read " + readBytes + " bytes");
        if (jvmZipReadBytes != -1 && zipReadBytes != -1) {
            assertOperationData(aggregates, OperationSource.JVM,
                    OperationCategory.ZIP, jvmZipReadBytes + zipReadBytes,
                    jvmZipReadBytes + zipReadBytes);
            System.err.println("Read " + zipReadBytes + " zip bytes");
            System.err.println("Read " + jvmZipReadBytes + " jvm zip bytes");
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
