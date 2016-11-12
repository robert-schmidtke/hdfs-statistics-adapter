/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.agent;

import java.io.FileDescriptor;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps file descriptors to file names and checks these mappings against a
 * user-defined blacklist of file names.
 * 
 * @author robert
 *
 */
public class FileDescriptorBlacklist {

    private final Map<FileDescriptor, String> fileDescriptors;

    private final Set<String> blacklistedFilenames;

    public FileDescriptorBlacklist() {
        fileDescriptors = new ConcurrentHashMap<FileDescriptor, String>();
        blacklistedFilenames = ConcurrentHashMap.<String> newKeySet();
    }

    public void addFileDescriptor(FileDescriptor fileDescriptor, String filename) {
        fileDescriptors.put(fileDescriptor, filename);
    }

    public void addBlacklistedFilename(String filename) {
        blacklistedFilenames.add(filename);
    }

    /**
     * Returns true if {@code fileDescriptor} has not been mapped to a file name
     * before, or its mapping has been blacklisted via
     * {@link #addBlacklistedFilename(String)}.
     * 
     * @param fileDescriptor
     * @return
     */
    public boolean isBlacklisted(FileDescriptor fileDescriptor) {
        String filename = fileDescriptors.get(fileDescriptor);
        return filename == null || blacklistedFilenames.contains(filename);
    }

}
