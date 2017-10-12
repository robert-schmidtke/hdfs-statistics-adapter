/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.util;

import java.lang.reflect.Constructor;

public class Unsafe {

    @SuppressWarnings("restriction")
    public static final sun.misc.Unsafe U;
    static {
        try {
            @SuppressWarnings("restriction")
            Constructor<sun.misc.Unsafe> unsafeConstructor = sun.misc.Unsafe.class
                    .getDeclaredConstructor();
            unsafeConstructor.setAccessible(true);
            U = unsafeConstructor.newInstance();
        } catch (Exception e) {
            throw new Error(e);
        }
    }

}
