/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;
import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;

public class FileOutputStreamCallback {

    // the LiveOperationStatisticsAggregator uses FileOutputStream to write its
    // logs
    private static String LOG_FILE_PREFIX = null;

    // set to true if all calls made to this callback should be discarded
    private boolean discard = false;

    public void openCallback(long startTime, long endTime, String filename) {
        // null as long as the LiveOperationStatisticsAggregator is not
        // initialized
        if (LOG_FILE_PREFIX == null) {
            LOG_FILE_PREFIX = LiveOperationStatisticsAggregator.instance
                    .getLogFilePrefix();
        }

        // discard all writes to the LiveOperationStatisticsAggregator's log
        // files
        discard = LOG_FILE_PREFIX != null && filename != null
                && filename.startsWith(LOG_FILE_PREFIX);
        if (!discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.JVM,
                            OperationCategory.OTHER, startTime, endTime);
        }
    }

    public void writeCallback(long startTime, long endTime) {
        if (!discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime, 1);
        }
    }

    public void writeBytesCallback(long startTime, long endTime, int len) {
        if (!discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime, len);
        }
    }

}
