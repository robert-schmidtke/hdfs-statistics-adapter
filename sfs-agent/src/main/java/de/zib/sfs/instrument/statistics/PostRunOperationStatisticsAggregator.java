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
        boolean delete = false;
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
            case "--delete":
                delete = true;
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
        final boolean deleteFiles = delete;

        // for each source/category combination, create a thread that will
        // aggregate the relevant files, additional thread for file descriptor
        // mappings
        Thread[] aggregatorThreads = new Thread[OperationSource.VALUES.length
                * OperationCategory.VALUES.length + 1];
        int threadId = 0;

        // list all files before we add the aggregated ones to them
        final String[] csvFileNames = path.list();

        for (final OperationSource source : OperationSource.VALUES) {
            for (final OperationCategory category : OperationCategory.VALUES) {
                aggregatorThreads[threadId++] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // list all files matching this source/category
                        // combination
                        String sourceName = source.name().toLowerCase();
                        String categoryName = category.name().toLowerCase();

                        // 1 MB read buffer for copying log files
                        char[] readBuf = new char[1024 * 1024];

                        BufferedWriter writer = null;
                        for (String csvFileName : csvFileNames) {
                            if (!csvFileName.contains(sourceName)
                                    || !csvFileName.contains(categoryName)) {
                                continue;
                            }

                            BufferedReader reader = null;
                            File f = new File(path, csvFileName);
                            try {
                                reader = new BufferedReader(new FileReader(f));
                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                                continue;
                            }

                            try {
                                String header = reader.readLine();
                                if (header == null) {
                                    // empty file, skip
                                    if (deleteFiles) {
                                        f.delete();
                                    }
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

                                // New line to account for not fully written
                                // CSVs. This way, the next file gets appended
                                // to the new line, rather than the current one.
                                // This simplifies later parsing, as not fully
                                // written lines are easier to deal with than
                                // overfull ones.
                                writer.newLine();
                                writer.flush();

                                reader.close();
                                reader = null;

                                if (deleteFiles) {
                                    f.delete();
                                }
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

        // copy pasta for the file descriptor mappings
        aggregatorThreads[threadId] = new Thread(new Runnable() {
            @Override
            public void run() {
                String[] fileDescriptorMappingFileNames = path
                        .list(new FilenameFilter() {
                            @Override
                            public boolean accept(File dir, String name) {
                                return name.contains("filedescriptormappings");
                            }
                        });

                char[] readBuf = new char[1024 * 1024];
                BufferedWriter writer = null;
                for (String fileDescriptorMappingFileName : fileDescriptorMappingFileNames) {
                    BufferedReader reader = null;
                    File f = new File(path, fileDescriptorMappingFileName);
                    try {
                        reader = new BufferedReader(new FileReader(f));
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                        continue;
                    }

                    try {
                        String header = reader.readLine();
                        if (header == null) {
                            if (deleteFiles) {
                                f.delete();
                            }
                            continue;
                        }

                        if (writer == null) {
                            writer = new BufferedWriter(new FileWriter(new File(
                                    path, prefix + "filedescriptormappings"
                                            + suffix + ".csv")));
                            writer.write(header);
                            writer.newLine();
                        }

                        int numRead = -1;
                        while ((numRead = reader.read(readBuf)) != -1) {
                            writer.write(readBuf, 0, numRead);
                        }
                        writer.newLine();
                        writer.flush();
                        reader.close();
                        reader = null;

                        if (deleteFiles) {
                            f.delete();
                        }
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
