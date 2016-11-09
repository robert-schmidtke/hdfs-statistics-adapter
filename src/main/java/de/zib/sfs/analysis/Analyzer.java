/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Analyzer {

    /**
     * Map from hostname to a map from time bin to a map from operation name to
     * aggregate operation information (i.e. aggregate information per host).
     */
    private static Map<String, Map<Long, Map<String, OperationInfo.Aggregator>>> aggregateHostOperationInfos;

    /**
     * Map from time bin to a map from operation name to aggregate operation
     * information (i.e. aggregate information for all hosts).
     */
    private static Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfos;

    public static void main(String[] args) throws IOException {
        aggregateHostOperationInfos = new HashMap<String, Map<Long, Map<String, OperationInfo.Aggregator>>>();
        aggregateOperationInfos = new TreeMap<Long, Map<String, OperationInfo.Aggregator>>();

        File logFileDirectory = new File(args[0]);
        final String fileNamePattern = args[1];
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(fileNamePattern);
            }
        };

        File[] logFiles = logFileDirectory.listFiles(filter);

        // get total amount of data so we can estimate remaining time
        long totalSize = 0L, currentSize = 0L;
        for (File logFile : logFiles) {
            totalSize += logFile.length();
        }
        long startTime = System.currentTimeMillis(), duration = 0L;

        long minTimeBin = Long.MAX_VALUE;
        for (File logFile : logFileDirectory.listFiles(filter)) {
            System.out.println("Analyzing " + logFile.getAbsolutePath());

            BufferedReader in = new BufferedReader(new FileReader(logFile));
            String line = null;
            while ((line = in.readLine()) != null) {
                OperationInfo operationInfo = OperationInfoFactory
                        .parseFromLogLine(line);

                // aggregate information per host
                Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfosPerHost = aggregateHostOperationInfos
                        .get(operationInfo.getHostname());
                if (aggregateOperationInfosPerHost == null) {
                    aggregateOperationInfosPerHost = new TreeMap<Long, Map<String, OperationInfo.Aggregator>>();
                    aggregateHostOperationInfos.put(
                            operationInfo.getHostname(),
                            aggregateOperationInfosPerHost);
                }

                // and per host per second
                long timeBin = Math.floorDiv(operationInfo.getStartTime(),
                        1000L);
                minTimeBin = Math.min(minTimeBin, timeBin);

                // add the aggregator to the per-host and overall
                for (Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfos : Arrays
                        .asList(aggregateOperationInfosPerHost,
                                aggregateOperationInfos)) {
                    Map<String, OperationInfo.Aggregator> aggregateOperationInfosPerTime = aggregateOperationInfos
                            .get(timeBin);
                    if (aggregateOperationInfosPerTime == null) {
                        aggregateOperationInfosPerTime = new TreeMap<String, OperationInfo.Aggregator>();
                        aggregateOperationInfos.put(timeBin,
                                aggregateOperationInfosPerTime);
                    }

                    // and per host per second per operation
                    OperationInfo.Aggregator aggregator = aggregateOperationInfosPerTime
                            .get(operationInfo.getName());
                    if (aggregator == null) {
                        aggregator = operationInfo.getAggregator();
                        aggregateOperationInfosPerTime.put(
                                operationInfo.getName(), aggregator);
                    }
                    aggregator.aggregate(operationInfo);
                }
            }
            in.close();

            // calculate some rudimentary progress
            currentSize += logFile.length();
            duration = System.currentTimeMillis() - startTime;
            double speed = (double) currentSize / (double) duration;
            long remainingSize = totalSize - currentSize;
            long remainingTime = (long) (remainingSize / (speed * 1000));

            System.out.println(remainingTimeString(remainingTime) + " left ("
                    + (long) (speed * 1000 / 1048576) + " MB/s)");
        }

        // add the overall information as special host
        aggregateHostOperationInfos.put("all", aggregateOperationInfos);

        for (Map.Entry<String, Map<Long, Map<String, OperationInfo.Aggregator>>> aggregateOperationInfosPerHost : aggregateHostOperationInfos
                .entrySet()) {
            String hostname = aggregateOperationInfosPerHost.getKey();
            System.out.println("Writing data for " + hostname);

            BufferedWriter out = new BufferedWriter(new FileWriter(new File(
                    logFileDirectory, hostname + ".csv")));

            boolean writeHeaders = true;
            for (Map.Entry<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfosPerTime : aggregateOperationInfosPerHost
                    .getValue().entrySet()) {
                long timeBin = aggregateOperationInfosPerTime.getKey();
                if (writeHeaders) {
                    out.write("time");
                    out.write(",reads,localReads");
                    out.write(",totalReadTime,minReadTime,maxReadTime");
                    out.write(",totalReadData,minReadData,maxReadData");
                    out.write(",writes");
                    out.write(",totalWriteTime,minWriteTime,maxWriteTime");
                    out.write(",totalWriteData,minWriteData,maxWriteData");
                    out.write("\n");
                    writeHeaders = false;
                } else {
                    out.write(Long.toString(timeBin - minTimeBin));

                    ReadDataOperationInfo.Aggregator readAggregator = (ReadDataOperationInfo.Aggregator) aggregateOperationInfosPerTime
                            .getValue().get("read");
                    if (readAggregator != null) {
                        out.write("," + readAggregator.getCount());
                        out.write("," + readAggregator.getLocalCount());
                        out.write("," + readAggregator.getDuration());
                        out.write("," + readAggregator.getMinDuration());
                        out.write("," + readAggregator.getMaxDuration());
                        out.write("," + readAggregator.getData());
                        out.write("," + readAggregator.getMinData());
                        out.write("," + readAggregator.getMaxData());
                    } else {
                        out.write(",0,0,0,0,0,0,0,0");
                    }

                    DataOperationInfo.Aggregator writeAggregator = (DataOperationInfo.Aggregator) aggregateOperationInfosPerTime
                            .getValue().get("write");
                    if (writeAggregator != null) {
                        out.write("," + writeAggregator.getCount());
                        out.write("," + writeAggregator.getDuration());
                        out.write("," + writeAggregator.getMinDuration());
                        out.write("," + writeAggregator.getMaxDuration());
                        out.write("," + writeAggregator.getData());
                        out.write("," + writeAggregator.getMinData());
                        out.write("," + writeAggregator.getMaxData());
                    } else {
                        out.write(",0,0,0,0,0,0,0");
                    }

                    out.write("\n");
                }
            }
            out.close();
        }
    }

    private static String remainingTimeString(long remainingTimeSeconds) {
        long hours = remainingTimeSeconds / 3600;
        long minutes = (remainingTimeSeconds % 3600) / 60;
        long seconds = (remainingTimeSeconds % 3600) % 60;
        return hours + "h " + minutes + "m " + seconds + "s";
    }
}
