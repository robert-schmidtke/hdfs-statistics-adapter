/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import de.zib.sfs.instrument.statistics.OperationStatistics.Aggregator.NotAggregatableException;

public class OperationStatisticsAggregator {

    private final String systemHostname, systemKey;
    private final int systemPid;

    private final long timeBinDuration;
    private final int timeBinCacheSize;

    private final String outputDirectory, outputSeparator;

    // for each source/category combination, map a time bin to an aggregator
    private final List<ConcurrentSkipListMap<Long, OperationStatistics.Aggregator>> aggregators;

    private final BufferedWriter[] writers;
    private final Object[] writerLocks;

    private static OperationStatisticsAggregator instance;

    public static OperationStatisticsAggregator getInstance() {
        if (instance == null) {
            try {
                instance = new OperationStatisticsAggregator();
            } catch (IllegalStateException e) {
                // swallow
            }
        }
        return instance;
    }

    private OperationStatisticsAggregator() {
        systemHostname = System.getProperty("de.zib.sfs.hostname");
        if (systemHostname == null) {
            throw new IllegalStateException("Agent not yet initialized");
        }

        systemPid = Integer.parseInt(System.getProperty("de.zib.sfs.pid"));
        systemKey = System.getProperty("de.zib.sfs.key");

        this.timeBinDuration = Long.parseLong(System
                .getProperty("de.zib.sfs.timeBin.duration"));
        this.timeBinCacheSize = Integer.parseInt(System
                .getProperty("de.zib.sfs.timeBin.cacheSize"));
        this.outputDirectory = System
                .getProperty("de.zib.sfs.output.directory");
        outputSeparator = ",";

        aggregators = new ArrayList<ConcurrentSkipListMap<Long, OperationStatistics.Aggregator>>();
        for (int i = 0; i < OperationSource.values().length
                * OperationCategory.values().length; ++i) {
            aggregators.add(new ConcurrentSkipListMap<>());
        }

        // similar for the writers
        writers = new BufferedWriter[OperationSource.values().length
                * OperationCategory.values().length];
        writerLocks = new Object[OperationSource.values().length
                * OperationCategory.values().length];
        for (OperationSource source : OperationSource.values()) {
            for (OperationCategory category : OperationCategory.values()) {
                writerLocks[getUniqueIndex(source, category)] = new Object();
            }
        }
    }

    public void aggregate(OperationStatistics operationStatistics) {
        // get the time bin applicable for this operation
        OperationStatistics.Aggregator aggregator = operationStatistics
                .getAggregator(timeBinDuration);
        aggregators
                .get(getUniqueIndex(aggregator.getSource(),
                        aggregator.getCategory())).merge(
                        aggregator.getTimeBin(), aggregator, (v1, v2) -> {
                            try {
                                return v1.aggregate(v2);
                            } catch (NotAggregatableException e) {
                                throw new IllegalArgumentException(e);
                            }
                        });

        // make sure to emit aggregates when the cache is full
        aggregators.forEach(v -> {
            for (int i = v.size() - timeBinCacheSize; i > 0; --i) {
                try {
                    write(v.remove(v.firstKey()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void shutdown() {
        aggregators.forEach(v -> {
            for (int i = v.size(); i > 0; --i) {
                try {
                    write(v.remove(v.firstKey()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        for (BufferedWriter writer : writers) {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void write(OperationStatistics.Aggregator aggregator)
            throws IOException {
        int index = getUniqueIndex(aggregator.getSource(),
                aggregator.getCategory());
        synchronized (writerLocks[index]) {
            if (writers[index] == null) {
                String filename = systemHostname + "." + systemPid + "."
                        + systemKey + "."
                        + aggregator.getSource().name().toLowerCase() + "."
                        + aggregator.getCategory().name().toLowerCase()
                        + ".csv";

                File file = new File(outputDirectory, filename);
                if (!file.exists()) {
                    writers[index] = new BufferedWriter(new FileWriter(file));
                    writers[index].write(aggregator
                            .getCsvHeaders(outputSeparator));
                    writers[index].newLine();
                } else {
                    throw new IOException(filename + " already exists");
                }
            }
            writers[index].write(aggregator.toCsv(outputSeparator));
            writers[index].newLine();
        }
    }

    private int getUniqueIndex(OperationSource source,
            OperationCategory category) {
        switch (source) {
        case JVM:
            switch (category) {
            case READ:
                return 0;
            case WRITE:
                return 1;
            case OTHER:
                return 2;
            default:
                throw new IllegalArgumentException(category.name());
            }
        case SFS:
            switch (category) {
            case READ:
                return 3;
            case WRITE:
                return 4;
            case OTHER:
                return 5;
            default:
                throw new IllegalArgumentException(category.name());
            }
        default:
            throw new IllegalArgumentException(source.name());
        }
    }
}
