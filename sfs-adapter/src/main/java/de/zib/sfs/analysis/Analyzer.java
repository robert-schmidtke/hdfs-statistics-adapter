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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Analyzer {

    /**
     * Map from hostname to a map from time bin to a map from operation name to
     * aggregate operation information (i.e. aggregate information per host).
     */
    private static Map<String, Map<Long, Map<String, OperationInfo.Aggregator>>> aggregateOperationInfos;

    public static void main(String[] args) throws IOException {
        aggregateOperationInfos = new HashMap<String, Map<Long, Map<String, OperationInfo.Aggregator>>>();
        aggregateOperationInfos.put("all",
                new TreeMap<Long, Map<String, OperationInfo.Aggregator>>());

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

                // and per host per second
                long timeBin = Math.floorDiv(operationInfo.getStartTime(),
                        1000L);
                minTimeBin = Math.min(minTimeBin, timeBin);

                // add the aggregator to the per-host and overall
                for (String hostname : new String[] {
                        operationInfo.getHostname(), "all" }) {
                    // get the appropriate hostname information
                    Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfosPerHost = getOperationInfosPerHost(hostname);

                    // get the appropriate time information for this host
                    Map<String, OperationInfo.Aggregator> aggregateOperationInfosPerHostPerTime = getOperationInfosPerHostPerTime(
                            aggregateOperationInfosPerHost, timeBin);

                    // get the appropriate operation information for this host
                    // and time
                    OperationInfo.Aggregator operationInfoAggregator = getOperationInfoAggregator(
                            aggregateOperationInfosPerHostPerTime,
                            operationInfo, operationInfo.getName());
                    operationInfoAggregator.aggregate(operationInfo);
                }

                // if the operation was a non-local read, add it as special
                // remoteRead to the remote host and overall
                if (operationInfo instanceof ReadDataOperationInfo) {
                    ReadDataOperationInfo readDataOperationInfo = (ReadDataOperationInfo) operationInfo;
                    if (!readDataOperationInfo.isLocal()) {
                        for (String hostname : new String[] {
                                readDataOperationInfo.getRemoteHostname(),
                                "all" }) {
                            // same procedure as above, except set the operation
                            // name to "remoteRead"
                            Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfosPerRemoteHost = getOperationInfosPerHost(hostname);
                            Map<String, OperationInfo.Aggregator> aggregateOperationInfosPerRemoteHostPerTime = getOperationInfosPerHostPerTime(
                                    aggregateOperationInfosPerRemoteHost,
                                    timeBin);
                            OperationInfo.Aggregator operationInfoAggregator = getOperationInfoAggregator(
                                    aggregateOperationInfosPerRemoteHostPerTime,
                                    readDataOperationInfo, "remoteRead");
                            operationInfoAggregator
                                    .aggregate(readDataOperationInfo);
                        }
                    }
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

        for (Map.Entry<String, Map<Long, Map<String, OperationInfo.Aggregator>>> aggregateOperationInfosPerHost : aggregateOperationInfos
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
                    out.write(",remoteReads");
                    out.write(",totalRemoteReadTime,minRemoteReadTime,maxRemoteReadTime");
                    out.write(",totalRemoteReadData,minRemoteReadData,maxRemoteReadData");
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

                    ReadDataOperationInfo.Aggregator remoteReadAggregator = (ReadDataOperationInfo.Aggregator) aggregateOperationInfosPerTime
                            .getValue().get("remoteRead");
                    if (remoteReadAggregator != null) {
                        out.write("," + remoteReadAggregator.getCount());
                        out.write("," + remoteReadAggregator.getDuration());
                        out.write("," + remoteReadAggregator.getMinDuration());
                        out.write("," + remoteReadAggregator.getMaxDuration());
                        out.write("," + remoteReadAggregator.getData());
                        out.write("," + remoteReadAggregator.getMinData());
                        out.write("," + remoteReadAggregator.getMaxData());
                    } else {
                        out.write(",0,0,0,0,0,0,0");
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

    private static Map<Long, Map<String, OperationInfo.Aggregator>> getOperationInfosPerHost(
            String hostname) {
        Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfosPerHost = aggregateOperationInfos
                .get(hostname);
        if (aggregateOperationInfosPerHost == null) {
            aggregateOperationInfosPerHost = new TreeMap<Long, Map<String, OperationInfo.Aggregator>>();
            aggregateOperationInfos.put(hostname,
                    aggregateOperationInfosPerHost);
        }
        return aggregateOperationInfosPerHost;
    }

    private static Map<String, OperationInfo.Aggregator> getOperationInfosPerHostPerTime(
            Map<Long, Map<String, OperationInfo.Aggregator>> aggregateOperationInfosPerHost,
            long timeBin) {
        Map<String, OperationInfo.Aggregator> aggregateOperationInfosPerHostPerTime = aggregateOperationInfosPerHost
                .get(timeBin);
        if (aggregateOperationInfosPerHostPerTime == null) {
            aggregateOperationInfosPerHostPerTime = new TreeMap<String, OperationInfo.Aggregator>();
            aggregateOperationInfosPerHost.put(timeBin,
                    aggregateOperationInfosPerHostPerTime);
        }
        return aggregateOperationInfosPerHostPerTime;
    }

    private static OperationInfo.Aggregator getOperationInfoAggregator(
            Map<String, OperationInfo.Aggregator> aggregateOperationInfosPerHostPerTime,
            OperationInfo operationInfo, String operationName) {
        OperationInfo.Aggregator operationInfoAggregator = aggregateOperationInfosPerHostPerTime
                .get(operationName);
        if (operationInfoAggregator == null) {
            operationInfoAggregator = operationInfo.getAggregator();
            aggregateOperationInfosPerHostPerTime.put(operationName,
                    operationInfoAggregator);
        }
        return operationInfoAggregator;
    }

    private static String remainingTimeString(long remainingTimeSeconds) {
        long hours = remainingTimeSeconds / 3600;
        long minutes = (remainingTimeSeconds % 3600) / 60;
        long seconds = (remainingTimeSeconds % 3600) % 60;
        return hours + "h " + minutes + "m " + seconds + "s";
    }
}