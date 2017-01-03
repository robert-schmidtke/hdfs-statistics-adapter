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
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Stack;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import de.zib.sfs.analysis.OperationInfo;
import de.zib.sfs.analysis.OperationInfoFactory;

public class SfsInputFormat extends
        RichInputFormat<OperationInfo, SfsInputSplit> {

    private static final long serialVersionUID = -5409113748371926578L;

    private final String path;

    private final String[] hosts;

    private final int slotsPerHost;

    private final Stack<File> files = new Stack<File>();

    private boolean reachedEnd;

    private BufferedReader reader;

    public SfsInputFormat(String path, String[] hosts, int slotsPerHost) {
        this.path = path;
        this.hosts = hosts;
        this.slotsPerHost = slotsPerHost;
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
                ++i;
            }
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(SfsInputSplit[] inputSplits) {
        return new SfsInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(SfsInputSplit split) throws IOException {
        // obtain file list of target directory in deterministic order
        File[] files = new File(path).listFiles();
        Arrays.sort(files);

        // roughly assign the same number of files for each split
        for (int i = split.getLocalIndex(); i < files.length; i += slotsPerHost) {
            this.files.push(files[i]);
        }

        // check that we have at least one file to process
        reachedEnd = this.files.size() == 0;
        if (!reachedEnd) {
            reader = new BufferedReader(new FileReader(this.files.pop()));
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEnd;
    }

    @Override
    public OperationInfo nextRecord(OperationInfo reuse) throws IOException {
        String line = reader.readLine();

        // try the next file if possible
        while (line == null && !reachedEnd) {
            reader.close();
            reachedEnd = this.files.size() == 0;
            if (!reachedEnd) {
                reader = new BufferedReader(new FileReader(this.files.pop()));
                line = reader.readLine();
            }
        }

        if (line != null) {
            return OperationInfoFactory.parseFromLogLine(line);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

}
