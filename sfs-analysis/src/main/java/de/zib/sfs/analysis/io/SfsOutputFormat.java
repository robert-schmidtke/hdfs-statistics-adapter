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

import de.zib.sfs.analysis.statistics.OperationStatistics;

public class SfsOutputFormat extends
        RichOutputFormat<OperationStatistics.Aggregator> {

    private static final long serialVersionUID = -3809547644140153265L;

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsOutputFormat.class);

    private final String path;

    private final String separator;

    private BufferedWriter writer;

    public SfsOutputFormat(String path, String separator) {
        this.path = path;
        this.separator = separator;
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
        // lazily open file to correctly construct the file name, assuming all
        // incoming records have the same hostname, source and category
        if (writer == null) {
            File out = new File(path, record.getHostname() + "."
                    + record.getSource().name().toLowerCase() + "."
                    + record.getCategory().name().toLowerCase() + ".csv");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Opening file {} for writing", out);
            }
            writer = new BufferedWriter(new FileWriter(out));

            // write the CSV headers
            writer.write(record.getCsvHeaders(separator));
            writer.newLine();
        }

        // write the actual record
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
