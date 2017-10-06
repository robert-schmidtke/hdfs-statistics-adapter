/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument;

import java.io.FileDescriptor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractSfsCallback {

    // -1 means uninitialized
    protected int fd = -1;

    protected boolean skipOther = false;

    // for mapping FileDescriptors to our custom descriptors
    private static final Map<FileDescriptor, Integer> FILE_DESCRIPTOR_MAPPINGS = new ConcurrentHashMap<>();

    public void skipOther() {
        skipOther = true;
    }

    protected void putFileDescriptor(FileDescriptor fd1, int fd2) {
        FILE_DESCRIPTOR_MAPPINGS.putIfAbsent(fd1, fd2);
    }

    protected int getFileDescriptor(FileDescriptor fd) {
        // 0 means we don't have a name
        return FILE_DESCRIPTOR_MAPPINGS.getOrDefault(fd, 0);
    }

}
