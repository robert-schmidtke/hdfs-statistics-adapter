/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.agent;

import java.io.FileDescriptor;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private static final Log LOG = LogFactory
            .getLog(FileDescriptorBlacklist.class);

    public FileDescriptorBlacklist() {
        fileDescriptors = new ConcurrentHashMap<FileDescriptor, String>();
        blacklistedFilenames = ConcurrentHashMap.<String> newKeySet();
    }

    public void addFileDescriptor(FileDescriptor fileDescriptor, String filename) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding file descriptor: " + fileDescriptor + "->"
                    + filename);
        }
        fileDescriptors.put(fileDescriptor, filename);
    }

    public void addBlacklistedFilename(String filename) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Blacklisting file name: " + filename);
        }
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

    public Set<FileDescriptor> getFileDescriptors() {
        return Collections.unmodifiableSet(fileDescriptors.keySet());
    }

    public String getFilename(FileDescriptor fileDescriptor) {
        return fileDescriptors.get(fileDescriptor);
    }

}
