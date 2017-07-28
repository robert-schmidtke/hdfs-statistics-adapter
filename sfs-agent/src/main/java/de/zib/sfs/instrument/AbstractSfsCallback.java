/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

public abstract class AbstractSfsCallback {

    protected int fd = -1;

    protected boolean skipOther = false;

    public void skipOther() {
        skipOther = true;
    }

}
