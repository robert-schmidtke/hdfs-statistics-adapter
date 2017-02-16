/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

public class PostRunOperationStatisticsAggregator {

    public static void main(String[] args) {

        int pathIndex = -1, prefixIndex = -1, suffixIndex = -1;
        for (int i = 0; i < args.length; ++i) {
            switch (args[i]) {
            case "--path":
                pathIndex = ++i;
                break;
            case "--prefix":
                prefixIndex = ++i;
                break;
            case "--suffix":
                suffixIndex = ++i;
                break;
            default:
                System.err.println("Unexpected argument: " + args[i]);
                System.exit(1);
            }
        }

        if (pathIndex == -1) {
            System.err.println("Missing argument --path <path>");
            System.exit(1);
        }

        final File path = new File(args[pathIndex]);
        if (!path.exists()) {
            System.err.println("Path '" + path + "' does not exist");
            System.exit(1);
        }

        final String prefix = prefixIndex == -1 ? "" : args[prefixIndex];
        final String suffix = suffixIndex == -1 ? "" : args[suffixIndex];

        // for each source/category combination, create a thread that will
        // aggregate the relevant files
        Thread[] aggregatorThreads = new Thread[OperationSource.values().length
                * OperationCategory.values().length];
        int threadId = 0;
        for (final OperationSource source : OperationSource.values()) {
            for (final OperationCategory category : OperationCategory
                    .values()) {
                aggregatorThreads[threadId++] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // list all files matching this source/category
                        // combination
                        String sourceName = source.name().toLowerCase();
                        String categoryName = category.name().toLowerCase();
                        String[] csvFileNames = path.list(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return name.contains(sourceName)
                                        && name.contains(categoryName);
                            }
                        });

                        // 1 MB read buffer for copying log files
                        char[] readBuf = new char[1024 * 1024];

                        BufferedWriter writer = null;
                        for (String csvFileName : csvFileNames) {
                            BufferedReader reader = null;
                            try {
                                reader = new BufferedReader(new FileReader(
                                        new File(path, csvFileName)));
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                                continue;
                            }

                            try {
                                String header = reader.readLine();
                                if (header == null) {
                                    // empty file, skip
                                    continue;
                                }

                                // create one aggregate file for each
                                // source/category combination
                                if (writer == null) {
                                    writer = new BufferedWriter(new FileWriter(
                                            new File(path, prefix + sourceName
                                                    + "-" + categoryName
                                                    + suffix + ".csv")));

                                    // write header only once
                                    writer.write(header);
                                    writer.newLine();
                                }

                                // copy the rest in bigger chunks
                                int numRead = -1;
                                while ((numRead = reader.read(readBuf)) != -1) {
                                    writer.write(readBuf, 0, numRead);
                                }
                                writer.flush();

                                reader.close();
                                reader = null;
                            } catch (IOException e) {
                                e.printStackTrace();
                                continue;
                            }
                        }

                        if (writer != null) {
                            try {
                                writer.close();
                                writer = null;
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
        }

        for (Thread t : aggregatorThreads) {
            t.start();
        }

        for (Thread t : aggregatorThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
