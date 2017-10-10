/*
 * Copyright (c) 2017 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.instrument.statistics;

public class FileDescriptorMapping {

    private String hostname, key;
    private int pid;

    private int fd;
    private String path;

    public FileDescriptorMapping() {

    }

    public FileDescriptorMapping(String hostname, int pid, String key, int fd,
            String path) {
        this.hostname = hostname;
        this.pid = pid;
        this.key = key;
        this.fd = fd;
        this.path = path;
    }

    public String getHostname() {
        return this.hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getPid() {
        return this.pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public int getFd() {
        return this.fd;
    }

    public void setFd(int fd) {
        this.fd = fd;
    }

    public String getPath() {
        return this.path;
    }

    public void setPath(String path) {
        this.path = path;
    }

}
