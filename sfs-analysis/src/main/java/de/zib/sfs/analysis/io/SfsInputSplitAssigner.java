/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.io;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.flink.core.io.InputSplitAssigner;

public class SfsInputSplitAssigner implements InputSplitAssigner {

    private final Map<String, ConcurrentLinkedDeque<SfsInputSplit>> unassignedInputSplits;

    public SfsInputSplitAssigner(SfsInputSplit[] inputSplits) {
        unassignedInputSplits = new HashMap<>();
        for (SfsInputSplit inputSplit : inputSplits) {
            unassignedInputSplits.computeIfAbsent(inputSplit.getHost(),
                    host -> new ConcurrentLinkedDeque<>()).push(inputSplit);
        }
    }

    @Override
    public SfsInputSplit getNextInputSplit(String host, int taskId) {
        try {
            return unassignedInputSplits.get(host).pop();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

}
