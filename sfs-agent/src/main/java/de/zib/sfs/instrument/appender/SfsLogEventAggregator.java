/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.appender;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.OperationStatistics;
import de.zib.sfs.instrument.statistics.OperationStatistics.Aggregator.NotAggregatableException;
import de.zib.sfs.instrument.statistics.OperationStatisticsFactory;

/**
 * Aggregator that takes formatted log events generated by SFS and combines
 * them. It is assumed the incoming events all have the same hostname, PID and
 * key set. This is because this aggregator is supposed to run within the same
 * JVM that generates the log events, and there is only ever one JVM per
 * hostname/pid/key combination. These three properties are read from the system
 * configuration.
 * 
 * @author robert
 *
 */
public class SfsLogEventAggregator {

    // keep track of system wide properties so we can detect deviations in the
    // incoming log messages
    private final String systemHostname, systemKey;
    private final int systemPid;

    private final ParserWorker parserWorker;
    private final Thread parserWorkerThread;

    private final AggregatorWorker[] aggregatorWorkers;
    private final Thread[] aggregatorWorkerThreads;

    public SfsLogEventAggregator(final long timeBinDuration,
            final int timeBinCacheSize, final String outputDirectory,
            final String outputSeparator) {
        systemHostname = System.getProperty("de.zib.sfs.hostname");
        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        parserWorker = new ParserWorker();
        parserWorkerThread = new Thread(parserWorker);

        // keep a sparse array of workers, where only specific indices are set
        if (OperationSource.values().length > 2) {
            throw new IllegalStateException("Too many possible sources");
        }

        if (OperationCategory.values().length > 3) {
            throw new IllegalStateException("Too many possible categories");
        }

        aggregatorWorkers = new AggregatorWorker[7];
        aggregatorWorkerThreads = new Thread[6];
        int threadIndex = 0;
        for (OperationSource source : OperationSource.values()) {
            for (OperationCategory category : OperationCategory.values()) {
                // for two sources and three categories, index will be one of:
                // 0 (00 00), 1 (00 01), 2 (00 10),
                // 4 (10 00), 5 (10 01), 6 (10 10)
                byte index = (byte) (source.ordinal() << 2 | category.ordinal());
                aggregatorWorkers[index] = new AggregatorWorker(
                        timeBinDuration, timeBinCacheSize, outputDirectory,
                        outputSeparator);
                aggregatorWorkerThreads[threadIndex++] = new Thread(
                        aggregatorWorkers[index]);
            }
        }
    }

    public void start() {
        for (Thread t : aggregatorWorkerThreads) {
            t.start();
        }
        parserWorkerThread.start();
    }

    public void stop(long millis) throws InterruptedException {
        parserWorker.stop();
        parserWorkerThread.join(millis);

        for (AggregatorWorker w : aggregatorWorkers) {
            // index 3 is not set
            if (w != null) {
                w.stop();
            }
        }

        for (Thread t : aggregatorWorkerThreads) {
            t.join(millis);
        }
    }

    public void append(String logEntry) {
        parserWorker.offer(logEntry);
    }

    private class ParserWorker implements Runnable {

        private final BlockingQueue<String> queue;

        private boolean stop;

        public ParserWorker() {
            queue = new LinkedBlockingQueue<>();
            stop = false;
        }

        public void offer(String logEntry) {
            queue.offer(logEntry);
        }

        @Override
        public void run() {
            while (queue.size() > 0 || !stop) {
                String logEntry;
                try {
                    logEntry = queue.poll(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logEntry = null;
                }

                if (logEntry != null) {
                    // parse the entry and assign it to the appropriate worker
                    OperationStatistics operationStatistics = OperationStatisticsFactory
                            .parseFromLogLine(logEntry);
                    byte index = (byte) (operationStatistics.getSource()
                            .ordinal() << 2 | operationStatistics.getCategory()
                            .ordinal());
                    aggregatorWorkers[index].offer(operationStatistics);
                }
            }
        }

        public void stop() {
            stop = true;
        }

    }

    private class AggregatorWorker implements Runnable {

        private final BlockingQueue<OperationStatistics> queue;

        private boolean stop;

        private final TreeMap<Long, OperationStatistics.Aggregator> aggregators;

        private final long timeBinDuration;

        private final int timeBinCacheSize;

        private final String outputDirectory, outputSeparator;

        private BufferedWriter writer;

        public AggregatorWorker(long timeBinDuration, int timeBinCacheSize,
                String outputDirectory, String outputSeparator) {
            queue = new LinkedBlockingQueue<>();
            stop = false;
            aggregators = new TreeMap<>();
            this.timeBinDuration = timeBinDuration;
            this.timeBinCacheSize = timeBinCacheSize;
            this.outputDirectory = outputDirectory;
            this.outputSeparator = outputSeparator;
            writer = null;
        }

        public void offer(OperationStatistics operationStatistics) {
            queue.offer(operationStatistics);
        }

        @Override
        public void run() {
            while (queue.size() > 0 || !stop) {
                OperationStatistics operationStatistics;
                try {
                    operationStatistics = queue.poll(1000,
                            TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    operationStatistics = null;
                }

                if (operationStatistics != null) {
                    // get the time bin applicable for this operation
                    OperationStatistics.Aggregator aggregator = operationStatistics
                            .getAggregator(timeBinDuration);
                    OperationStatistics.Aggregator timeBinAggregator = aggregators
                            .get(aggregator.getTimeBin());
                    if (timeBinAggregator == null) {
                        // add new bin if we have the space
                        if (aggregators.size() < timeBinCacheSize) {
                            aggregators
                                    .put(aggregator.getTimeBin(), aggregator);
                        } else {
                            continue;
                        }
                    } else {
                        try {
                            timeBinAggregator.aggregate(aggregator);
                        } catch (NotAggregatableException e) {
                            e.printStackTrace();
                            continue;
                        }
                    }

                    // make sure to emit aggregates when the cache is full
                    while (aggregators.size() >= timeBinCacheSize) {
                        try {
                            write(aggregators.remove(aggregators.firstKey()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }

            // flush remaining aggregates
            while (aggregators.size() > 0) {
                try {
                    write(aggregators.remove(aggregators.firstKey()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private void write(OperationStatistics.Aggregator aggregator)
                throws IOException {
            if (writer == null) {
                String filename = systemHostname + "." + systemPid + "."
                        + systemKey + "."
                        + aggregator.getSource().name().toLowerCase() + "."
                        + aggregator.getCategory().name().toLowerCase()
                        + ".csv";
                writer = new BufferedWriter(new FileWriter(new File(
                        outputDirectory, filename)));
                writer.write(aggregator.getCsvHeaders(outputSeparator));
                writer.newLine();
            }

            writer.write(aggregator.toCsv(outputSeparator));
            writer.newLine();
        }

        public void stop() {
            stop = true;
        }

    }

}
