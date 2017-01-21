/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.statistics.OperationStatistics;

public class SfsOutputFormat extends
        RichOutputFormat<OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -3809547644140153265L;

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsOutputFormat.class);

    private final String path;

    private final String separator;

    private final Map<String, BufferedWriter> writers;

    public SfsOutputFormat(String path, String separator) {
        this.path = path;
        this.separator = separator;
        writers = new HashMap<>();
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
    }

    @Override
    public void writeRecord(OperationStatistics.Aggregator record)
            throws IOException {
        // construct target filename
        String filename = record.getHostname() + "." + record.getPid() + "."
                + record.getKey() + "."
                + record.getSource().name().toLowerCase() + "."
                + record.getCategory().name().toLowerCase() + ".csv";

        // construct writer, make sure to print the CSV headers on first write
        BufferedWriter writer = writers.computeIfAbsent(filename, k -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Opening file {} for writing", k);
            }

            try {
                BufferedWriter w = new BufferedWriter(new FileWriter(new File(
                        path, filename)));
                w.write(record.getCsvHeaders(separator));
                return w;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        // write the actual record
        writer.write(record.toCsv(separator));
        writer.newLine();
    }

    @Override
    public void close() throws IOException {
        // close all open writers
        writers.forEach((k, v) -> {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing file {}", k);
            }

            try {
                v.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
