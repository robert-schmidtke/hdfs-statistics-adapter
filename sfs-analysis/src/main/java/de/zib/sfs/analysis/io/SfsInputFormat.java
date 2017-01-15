/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;
import java.util.zip.GZIPInputStream;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zib.sfs.analysis.statistics.OperationStatistics;
import de.zib.sfs.analysis.statistics.OperationStatisticsFactory;

public class SfsInputFormat extends
        RichInputFormat<OperationStatistics, SfsInputSplit> {

    private static final long serialVersionUID = -5409113748371926578L;

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsInputFormat.class);

    private final String path, prefix;

    private final String[] hosts;

    private final int slotsPerHost;

    private int localIndex;

    private String host;

    private final Stack<File> files = new Stack<File>();

    private boolean reachedEnd;

    private BufferedReader reader;

    public SfsInputFormat(String path, String prefix, String[] hosts,
            int slotsPerHost) {
        this.path = path;
        this.prefix = prefix;
        this.hosts = hosts;
        this.slotsPerHost = slotsPerHost;
        localIndex = -1;
        reachedEnd = false;
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SfsInputSplit[] createInputSplits(int minNumSplits)
            throws IOException {
        // create the same number of splits per host
        SfsInputSplit[] splits = new SfsInputSplit[hosts.length * slotsPerHost];
        int i = 0;
        for (String host : hosts) {
            for (int slot = 0; slot < slotsPerHost; ++slot) {
                splits[i] = new SfsInputSplit(i, host, slot);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created split: {}", splits[i]);
                }
                ++i;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created {} splits", i);
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(SfsInputSplit[] inputSplits) {
        return new SfsInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(SfsInputSplit split) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Opening split {} on host {}", split.getLocalIndex(),
                    split.getHost());
        }

        if (localIndex != -1) {
            throw new IOException("localIndex is already assigned: "
                    + localIndex);
        }
        localIndex = split.getLocalIndex();
        host = split.getHost();

        String hostname = getHostname();
        if (!host.equals(hostname)) {
            throw new IllegalStateException("Unexpected host " + host
                    + " for split on host " + hostname);
        }

        // obtain file list of target directory in deterministic order
        File[] files = new File(path).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return prefix == null || name.startsWith(prefix);
            }
        });

        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                // larger files first, use regular comparison for equally sized
                // files
                return o1.length() > o2.length() ? -1 : o1.length() < o2
                        .length() ? 1 : o1.compareTo(o2);
            }
        });

        // roughly assign the same number of files for each split
        for (int i = split.getLocalIndex(); i < files.length; i += slotsPerHost) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding file {} for split {} on host {}", files[i],
                        split.getLocalIndex(), split.getHost());
            }
            this.files.push(files[i]);
        }

        // check that we have at least one file to process
        reachedEnd = this.files.size() == 0;
        if (!reachedEnd) {
            openNextFile();
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEnd;
    }

    @Override
    public OperationStatistics nextRecord(OperationStatistics reuse)
            throws IOException {
        String line = reader.readLine();

        // try the next file if possible
        while (line == null && !reachedEnd) {
            reader.close();
            reachedEnd = this.files.size() == 0;
            if (!reachedEnd) {
                openNextFile();
                line = reader.readLine();
            }
        }

        if (line != null) {
            try {
                OperationStatistics statistics = OperationStatisticsFactory
                        .parseFromLogLine(line);
                statistics.setInternalId(localIndex);
                if (!host.equals(statistics.getHostname())) {
                    throw new IllegalStateException("Unexpected host "
                            + statistics.getHostname() + " for record on host "
                            + host);
                }
                return statistics;
            } catch (Exception e) {
                throw new IOException("Error parsing log line: " + line, e);
            }
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing split {} on host {}", localIndex, host);
        }

        if (reader != null) {
            reader.close();
            reader = null;
        }
        localIndex = -1;
        host = null;
    }

    // Helper methods

    private void openNextFile() throws FileNotFoundException, IOException {
        LOG.debug("Opening file {} for reading", this.files.peek());
        if (this.files.peek().getName().toLowerCase().endsWith(".gz")) {
            reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(this.files.pop()))));
        } else {
            reader = new BufferedReader(new FileReader(this.files.pop()));
        }
    }

    private static String getHostname() throws IOException {
        Process hostnameProcess = Runtime.getRuntime().exec("hostname");
        try {
            int exitCode = hostnameProcess.waitFor();
            if (exitCode != 0) {
                LOG.warn("'hostname' returned " + exitCode
                        + ", using $HOSTNAME instead.");
                return System.getenv("HOSTNAME");
            } else {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(hostnameProcess.getInputStream()));

                StringBuilder hostnameBuilder = new StringBuilder();
                String line = "";
                while ((line = reader.readLine()) != null) {
                    hostnameBuilder.append(line);
                }
                reader.close();
                return hostnameBuilder.toString();
            }
        } catch (InterruptedException e) {
            LOG.warn("Error executing 'hostname', using $HOSTNAME instead.", e);
            return System.getenv("HOSTNAME");
        }
    }

}
