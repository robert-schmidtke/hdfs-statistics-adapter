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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SfsInputSplitAssigner implements InputSplitAssigner {

    private static final Logger LOG = LoggerFactory
            .getLogger(SfsInputSplitAssigner.class);

    private final Map<String, ConcurrentLinkedDeque<SfsInputSplit>> unassignedInputSplits;

    public SfsInputSplitAssigner(SfsInputSplit[] inputSplits) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Assigning {} input splits", inputSplits.length);
        }

        unassignedInputSplits = new HashMap<>();
        for (SfsInputSplit inputSplit : inputSplits) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Assigning input split {} to host {}",
                        inputSplit.getSplitNumber(), inputSplit.getHost());
            }
            unassignedInputSplits.computeIfAbsent(inputSplit.getHost(),
                    host -> new ConcurrentLinkedDeque<>()).push(inputSplit);
        }
    }

    @Override
    public SfsInputSplit getNextInputSplit(String host, int taskId) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Getting next input split for host {} and task {}",
                        host, taskId);
            }

            if (!unassignedInputSplits.containsKey(host)) {
                // support localhost only for setups with one host
                if ("localhost".equals(host)
                        && unassignedInputSplits.size() == 1) {
                    SfsInputSplit unassignedInputSplit = unassignedInputSplits
                            .values().iterator().next().pop();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Assigning split {} to host {} instead of localhost",
                                unassignedInputSplit.getSplitNumber(),
                                unassignedInputSplit.getHost());
                    }
                    return unassignedInputSplit;
                } else {
                    throw new IllegalArgumentException("Invalid host: " + host);
                }
            } else {
                return unassignedInputSplits.get(host).pop();
            }
        } catch (NoSuchElementException e) {
            return null;
        }
    }

}
