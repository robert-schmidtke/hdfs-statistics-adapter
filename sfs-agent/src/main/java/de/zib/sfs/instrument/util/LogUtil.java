/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

public class LogUtil {

    private static boolean enableStderr = false;

    public static void enableStderrLogging(boolean enable) {
        LogUtil.enableStderr = enable;
    }

    public static void stderr(String format, Object... args) {
        if (enableStderr) {
            System.err.print(String.format(format, args));
        }
    }

}
