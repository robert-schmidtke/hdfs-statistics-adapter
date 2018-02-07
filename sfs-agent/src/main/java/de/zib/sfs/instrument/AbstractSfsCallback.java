/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

public abstract class AbstractSfsCallback {

    // -1 means uninitialized
    protected int fd = -1;

    protected boolean skipOther = false;

    // switch to set discard flag in next constructor invocations
    // per-thread to avoid concurrency issues
    public static final ThreadLocal<Boolean> DISCARD_NEXT = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.FALSE;
        }
    };

    // per-instance flag to signal whether to discard calls
    protected boolean discard = false;

    protected AbstractSfsCallback() {
        // if at the time of constructor invocation the above flag is set,
        // discard all calls
        this.discard = DISCARD_NEXT.get().booleanValue();
    }

    public void skipOther() {
        this.skipOther = true;
    }

}
