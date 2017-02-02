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
import java.nio.channels.FileChannel;
import java.util.Random;

/**
 * Basic test cases that cover the wrapped methods of FileInputStream,
 * FileOutputStream, RandomAccessFile and FileChannelImpl.
 * 
 * @author robert
 *
 */
public class InstrumentationTest {

    public static void main(String[] args) throws IOException {
        Random random = new Random();

        {
            File file = File.createTempFile("stream", null);

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

            FileOutputStream fos = new FileOutputStream(file);
            FileChannel fco = fos.getChannel();

            byte[] writeBuffer = new byte[1048576];
            for (int i = 0; i < 1048576; ++i) {
                writeBuffer[i] = (byte) random.nextInt(Byte.MAX_VALUE);
            }

            long numWritten = fco.write(ByteBuffer.wrap(writeBuffer));
            assert (numWritten == 1048576);

            numWritten = fco.write(new ByteBuffer[] {
                    ByteBuffer.wrap(writeBuffer), ByteBuffer.wrap(writeBuffer),
                    ByteBuffer.wrap(writeBuffer) });
            assert (numWritten == 3 * 1048576);
            fco.close();
            fos.close();

            FileInputStream fis = new FileInputStream(file);
            FileChannel fci = fis.getChannel();

            byte[] readBuffer = new byte[1048576];
            long numRead = fci.read(ByteBuffer.wrap(readBuffer));
            assert (numRead == 1048576);
            for (int i = 0; i < 1048576; ++i) {
                assert (readBuffer[i] == writeBuffer[i]);
            }

            byte[][] readBuffers = new byte[3][1048576];
            numRead = fci.read(new ByteBuffer[] {
                    ByteBuffer.wrap(readBuffers[0]),
                    ByteBuffer.wrap(readBuffers[1]),
                    ByteBuffer.wrap(readBuffers[2]) });
            assert (numRead == 3 * 1048576);
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

    }
}
