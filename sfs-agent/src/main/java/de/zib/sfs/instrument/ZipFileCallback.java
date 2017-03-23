/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.File;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class ZipFileCallback {

    public void openCallback(long startTime, long endTime, String filename) {
        long data = new File(filename).length();
        LiveOperationStatisticsAggregator.instance
                .aggregateReadDataOperationStatistics(OperationSource.JVM,
                        OperationCategory.READ, startTime, endTime, data,
                        false);
    }

}
