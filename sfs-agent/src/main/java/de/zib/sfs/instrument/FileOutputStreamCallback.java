/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

public class FileOutputStreamCallback extends AbstractSfsCallback {

    // the LiveOperationStatisticsAggregator uses FileOutputStream or
    // FileChannels obtained from FileOutputStreams to write its logs
    private static String LOG_FILE_PREFIX = null;

    // set to true if all calls made to this callback should be discarded
    private boolean discard = false;

    private FileOutputStream fos;

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
        this.discard = LOG_FILE_PREFIX != null && filename != null
                && filename.startsWith(LOG_FILE_PREFIX);
        if (!this.discard) {
            try {
                this.fd = LiveOperationStatisticsAggregator.instance
                        .registerFileDescriptor(filename, this.fos.getFD());
                this.fos = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (!this.skipOther) {
                LiveOperationStatisticsAggregator.instance
                        .aggregateOperationStatistics(OperationSource.JVM,
                                OperationCategory.OTHER, startTime, endTime,
                                this.fd);
            }
        } else {
            try {
                // useful for FileChannelImplCallback
                LiveOperationStatisticsAggregator.instance
                        .discardFileDescriptor(this.fos.getFD());
                this.fos = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void writeCallback(long startTime, long endTime) {
        getFileDescriptor();
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime,
                            this.fd, 1);
        }
    }

    public void writeBytesCallback(long startTime, long endTime, int len) {
        getFileDescriptor();
        if (!this.discard) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateDataOperationStatistics(OperationSource.JVM,
                            OperationCategory.WRITE, startTime, endTime,
                            this.fd, len);
        }
    }

    private void getFileDescriptor() {
        if (this.fd != -1 || this.discard) {
            return;
        }

        try {
            if (this.fos == null) {
                return;
            }
            synchronized (this) {
                if (this.fos == null) {
                    return;
                }

                FileDescriptor fileDescriptor = this.fos.getFD();
                this.discard |= FileDescriptor.out.equals(fileDescriptor)
                        || FileDescriptor.err.equals(fileDescriptor);
                this.fd = LiveOperationStatisticsAggregator.instance
                        .getFileDescriptor(fileDescriptor);
                this.fos = null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
