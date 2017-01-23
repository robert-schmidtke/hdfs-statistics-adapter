/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;

public class Log4j2BlockingShutdownUtil {

    public static void shutdownLog4j27(boolean verbose) {
        try {
            LogUtil.enableStderrLogging(verbose);

            // obtain the context for our logger
            org.apache.logging.log4j.Logger logger = LogManager
                    .getLogger("de.zib.sfs.AsyncLogger");
            if (logger instanceof Logger) {
                Logger l = (Logger) logger;
                LoggerContext c = l.getContext();

                // this shuts down the executor service, but does not wait for
                // its termination
                c.stop();
                ExecutorService executorService = c.getExecutorService();

                LogUtil.stderr(
                        "Waiting for ExecutorService '%s' of context '%s' of logger '%s' to shut down.\n",
                        executorService.toString(), c.toString(), l.toString());

                while (!executorService.awaitTermination(5000,
                        TimeUnit.MILLISECONDS)) {
                    LogUtil.stderr(
                            "Waiting for ExecutorService '%s' of context '%s' of logger '%s' to shut down after timeout.\n",
                            executorService.toString(), c.toString(),
                            l.toString());
                }

                LogUtil.stderr(
                        "Successfully shut down ExecutorService '%s' of context '%s' of logger '%s'.\n",
                        executorService.toString(), c.toString(), l.toString());
            } else {
                LogUtil.stderr("Cannot get context from logger '%s'.\n",
                        logger.toString());
            }
        } catch (Exception e) {
            LogUtil.stderr("Error shutting down Log4j2: %s.\n", e.getMessage());
        }
    }
}
