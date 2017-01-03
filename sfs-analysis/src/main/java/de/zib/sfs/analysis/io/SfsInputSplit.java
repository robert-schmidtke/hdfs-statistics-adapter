/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.analysis.io;

import org.apache.flink.core.io.InputSplit;

public class SfsInputSplit implements InputSplit {

    private static final long serialVersionUID = -8776981143654244187L;

    private final int splitNumber;

    private final String host;

    private final int localIndex;

    public SfsInputSplit(int splitNumber, String host, int localIndex) {
        this.splitNumber = splitNumber;
        this.host = host;
        this.localIndex = localIndex;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    public String getHost() {
        return host;
    }

    public int getLocalIndex() {
        return localIndex;
    }

}
