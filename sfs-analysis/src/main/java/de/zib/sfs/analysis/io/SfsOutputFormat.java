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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.statistics.OperationCategory;
import de.zib.sfs.analysis.statistics.OperationSource;
import de.zib.sfs.analysis.statistics.OperationStatistics;

public class SfsOutputFormat extends
        RichOutputFormat<OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -3809547644140153265L;

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsOutputFormat.class);

    private int taskNumber;

    private final String path;

    private final String separator;

    private BufferedWriter writer;

    private String hostname;

    private int pid;

    private String key;

    private OperationSource source;

    private OperationCategory category;

    public SfsOutputFormat(String path, String separator) {
        this.path = path;
        this.separator = separator;
        taskNumber = -1;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        if (this.taskNumber != -1) {
            throw new IllegalStateException(
                    "This output is already/still open for task"
                            + this.taskNumber);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening output for task {} / {}.", taskNumber, numTasks);
        }
        this.taskNumber = taskNumber;
    }

    @Override
    public void writeRecord(OperationStatistics.Aggregator record)
            throws IOException {
        // lazily open file to correctly construct the file name, assuming all
        // incoming records have the same hostname, pid, key, source and
        // category
        if (writer == null) {
            hostname = record.getHostname();
            pid = record.getPid();
            key = record.getKey();
            source = record.getSource();
            category = record.getCategory();

            File out = new File(path, hostname + "." + pid + "." + key + "."
                    + source.name().toLowerCase() + "."
                    + category.name().toLowerCase() + ".csv");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Opening file {} for writing", out);
            }
            writer = new BufferedWriter(new FileWriter(out));

            // write the CSV headers
            writer.write(record.getCsvHeaders(separator));
            writer.newLine();
        }

        if (!hostname.equals(record.getHostname())) {
            throw new IllegalArgumentException("Hostnames do not match: "
                    + hostname + ", " + record.getHostname());
        }

        if (pid != record.getPid()) {
            throw new IllegalArgumentException("Pids do not match: " + pid
                    + ", " + record.getPid());
        }

        if (!key.equals(record.getKey())) {
            throw new IllegalArgumentException("Keys do not match: " + key
                    + ", " + record.getKey());
        }

        if (!source.equals(record.getSource())) {
            throw new IllegalArgumentException("Sources do not match: "
                    + source + ", " + record.getSource());
        }

        if (!category.equals(record.getCategory())) {
            throw new IllegalArgumentException("Categories do not match: "
                    + category + ", " + record.getCategory());
        }

        // write the actual record
        writer.write(record.toCsv(separator));
        writer.newLine();
    }

    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing output for task {}.", taskNumber);
        }
        taskNumber = -1;

        if (writer != null) {
            writer.close();
            writer = null;
        }
        hostname = null;
        pid = -1;
        key = null;
        source = null;
        category = null;
    }
}
