/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileOutputStream;
import java.io.IOException;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class FileOutputStreamCallback extends AbstractSfsCallback {

    // the LiveOperationStatisticsAggregator uses FileOutputStream to write its
    // logs
    private static String LOG_FILE_PREFIX = null;

    // set to true if all calls made to this callback should be discarded
    private boolean discard = false;

    private final FileOutputStream fos;

    public FileOutputStreamCallback(FileOutputStream fos) {
        // fos.getFD() still throws at this point
        this.fos = fos;
    }

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
            fd = LiveOperationStatisticsAggregator.instance
                    .getFileDescriptor(filename);
            if (!skipOther) {
                LiveOperationStatisticsAggregator.instance
                        .aggregateOperationStatistics(OperationSource.JVM,
                                OperationCategory.OTHER, startTime, endTime,
                                fd);
            }

            // fos.getFD() is now safe
            putFileDescriptor();
        }
    }

    public void writeCallback(long startTime, long endTime) {
        if (!discard) {
            getFileDescriptor();
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime, fd, 1);
        }
    }

    public void writeBytesCallback(long startTime, long endTime, int len) {
        if (!discard) {
            getFileDescriptor();
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime, fd,
                            len);
        }
    }

    private void putFileDescriptor() {
        try {
            putFileDescriptor(fos.getFD(), fd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void getFileDescriptor() {
        if (fd != -1) {
            return;
        }

        try {
            fd = getFileDescriptor(fos.getFD());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
