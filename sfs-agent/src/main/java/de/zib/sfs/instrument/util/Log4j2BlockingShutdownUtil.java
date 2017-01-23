/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

public class Log4j2BlockingShutdownUtil {

    /**
     * Log4j2 2.7 uses executor services to execute asynchronous actions (like
     * compression). The default shutdown does not wait for submitted threads to
     * complete, possibly resulting in invalid gz archives, because compression
     * did not finish. Re-implement the
     * {@link org.apache.logging.log4j.LogManager#shutdown()} here, except wait
     * quite some time.
     * 
     * @param verbose
     * 
     * @see <a
     *      href="https://logging.apache.org/log4j/log4j-2.7/log4j-api/xref/org/apache/logging/log4j/LogManager.html">LogManager</a>
     * @see <a
     *      href="https://logging.apache.org/log4j/log4j-2.7/log4j-core/xref/org/apache/logging/log4j/core/LoggerContext.html">LoggerContext</a>
     * @see <a
     *      href="https://logging.apache.org/log4j/log4j-2.7/log4j-core/xref/org/apache/logging/log4j/core/util/ExecutorServices.html">ExecutorServices</a>
     * @see <a
     *      href="https://logging.apache.org/log4j/log4j-2.7/log4j-core/xref/org/apache/logging/log4j/core/appender/rolling/RollingFileManager.html">RollingFileManager</a>
     */
    public static void shutdownLog4j27(boolean verbose) {
        try {
            LogUtil.enableStderrLogging(verbose);

            // get current context
            org.apache.logging.log4j.spi.LoggerContext loggerContext = LogManager
                    .getContext(false);
            if (loggerContext != null && loggerContext instanceof LoggerContext) {
                // close() calls stop() as well, except with default arguments
                @SuppressWarnings("resource")
                LoggerContext lc = (LoggerContext) loggerContext;

                LogUtil.stderr("Stopping logger context '%s'.\n", lc.toString());

                // give the logger context some time to shut down
                long startTime = System.currentTimeMillis();
                if (lc.stop(600, TimeUnit.SECONDS)) {
                    LogUtil.stderr(
                            "Logger context '%s' stopped successfully after %d seconds.\n",
                            lc.toString(),
                            (System.currentTimeMillis() - startTime) / 1000L);
                } else {
                    if (Thread.currentThread().isInterrupted()) {
                        LogUtil.stderr(
                                "Interrupted while stopping logger context '%s' after %d seconds.\n",
                                lc.toString(),
                                (System.currentTimeMillis() - startTime) / 1000L);
                    } else {
                        LogUtil.stderr(
                                "Stopping logger context '%s' timed out after %d seconds.\n",
                                lc.toString(),
                                (System.currentTimeMillis() - startTime) / 1000L);
                    }
                }
            } else {
                LogUtil.stderr("Logger context '%s' cannot be stopped.\n",
                        loggerContext.toString());
            }
        } catch (Exception e) {
            LogUtil.stderr("Error shutting down Log4j2: %s.\n", e.getMessage());
        }
    }
}
