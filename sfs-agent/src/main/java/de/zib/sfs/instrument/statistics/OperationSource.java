/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import de.zib.sfs.instrument.statistics.fb.OperationSourceFB;

public enum OperationSource {
    JVM(OperationSourceFB.JVM), SFS(OperationSourceFB.SFS);

    private final byte flatBuffer;

    OperationSource(byte flatBuffer) {
        this.flatBuffer = flatBuffer;
    }

    public byte toFlatBuffer() {
        return flatBuffer;
    }

    public static OperationSource fromFlatBuffer(byte flatBuffer) {
        switch (flatBuffer) {
        case OperationSourceFB.JVM:
            return JVM;
        case OperationSourceFB.SFS:
            return SFS;
        default:
            throw new IllegalArgumentException("flatBuffer: " + flatBuffer);
        }
    }
}
