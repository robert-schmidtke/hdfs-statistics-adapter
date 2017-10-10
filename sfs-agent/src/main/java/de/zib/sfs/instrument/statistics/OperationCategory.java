/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

import de.zib.sfs.instrument.statistics.fb.OperationCategoryFB;

public enum OperationCategory {
    READ(OperationCategoryFB.Read), WRITE(OperationCategoryFB.Write), OTHER(
            OperationCategoryFB.Other), ZIP(OperationCategoryFB.Zip);

    private final byte flatBuffer;

    OperationCategory(byte flatBuffer) {
        this.flatBuffer = flatBuffer;
    }

    public byte toFlatBuffer() {
        return this.flatBuffer;
    }

    public static OperationCategory fromFlatBuffer(byte flatBuffer) {
        switch (flatBuffer) {
        case OperationCategoryFB.Read:
            return READ;
        case OperationCategoryFB.Write:
            return WRITE;
        case OperationCategoryFB.Other:
            return OTHER;
        case OperationCategoryFB.Zip:
            return ZIP;
        default:
            throw new IllegalArgumentException("flatBuffer: " + flatBuffer);
        }
    }
}
