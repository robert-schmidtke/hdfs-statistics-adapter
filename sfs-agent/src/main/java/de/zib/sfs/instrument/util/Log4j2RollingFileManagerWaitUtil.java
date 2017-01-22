/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.util.regex.Pattern;

public class Log4j2RollingFileManagerWaitUtil {

    // pattern of Log4j2 thread names
    private static final Pattern THREAD_NAME_PATTERN = Pattern
            .compile("Log4j2-TF-\\d+-RollingFileManager-\\d+");

    /**
     * Tries to obtain a list of the current thread group and waits for threads
     * started by the Log4j2 RollingFileManager.
     * 
     * @param verbose
     */
    public static void waitForRollingFileManager(boolean verbose) {
        try {
            LogUtil.enableStderrLogging(verbose);

            // obtain current thread group similarly to Log4jThreadFactory
            SecurityManager securityManager = System.getSecurityManager();
            ThreadGroup threadGroup = securityManager != null ? securityManager
                    .getThreadGroup() : Thread.currentThread().getThreadGroup();

            // get all active threads
            Thread[] activeThreads = new Thread[threadGroup.activeCount()];
            threadGroup.enumerate(activeThreads);

            // find Log4j2 RollingFileAppender threads and wait for them
            for (Thread thread : activeThreads) {
                String threadName = thread.getName();
                if (THREAD_NAME_PATTERN.matcher(threadName).matches()) {
                    try {
                        LogUtil.stderr("Waiting for thread '%s'.\n", threadName);
                        thread.join();
                    } catch (InterruptedException e) {
                        LogUtil.stderr("Error waiting for thread '%s': %s.\n",
                                threadName, e.getMessage());
                    }
                } else {
                    LogUtil.stderr("Skipping thread '%s'.\n", threadName);
                }
            }
        } catch (Exception e) {
            LogUtil.stderr("Error waiting for RollingFileManager: %s.\n",
                    e.getMessage());
        }
    }

}
