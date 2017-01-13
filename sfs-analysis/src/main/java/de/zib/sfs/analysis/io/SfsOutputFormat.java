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

import de.zib.sfs.analysis.OperationStatistics;

public class SfsOutputFormat extends
        RichOutputFormat<OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -3809547644140153265L;

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsOutputFormat.class);

    private final String path;

    private final String separator;

    private final String[] hosts;

    private final int slotsPerHost;

    private BufferedWriter writer;

    private boolean wroteHeaders;

    public SfsOutputFormat(String path, String separator, String[] hosts,
            int slotsPerHost) {
        this.path = path;
        this.separator = separator;
        this.hosts = hosts;
        this.slotsPerHost = slotsPerHost;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        if (writer != null) {
            throw new IOException("Reopening already opened SfsOutputFormat");
        }

        File out = new File(path + "." + taskNumber);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening file {} for writing", out);
        }
        writer = new BufferedWriter(new FileWriter(out));
        wroteHeaders = false;
    }

    @Override
    public void writeRecord(OperationStatistics.Aggregator record)
            throws IOException {
        if (!wroteHeaders) {
            writer.write(record.getCsvHeaders(separator));
            writer.newLine();
            wroteHeaders = true;
        }

        writer.write(record.toCsv(separator));
        writer.newLine();
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

}
