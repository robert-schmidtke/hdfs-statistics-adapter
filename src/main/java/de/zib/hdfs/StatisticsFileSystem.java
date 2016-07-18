/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class StatisticsFileSystem extends FileSystem {

    private URI fileSystemUri;

    private FileSystem wrappedFS;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        wrappedFS = get(
                URI.create(getConf().get("sfs.wrappedFS", "hdfs") + "://"
                        + name.getAuthority()), getConf());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Wrapping file system with scheme '" + wrappedFS + "'.");
        }

        fileSystemUri = URI.create(getScheme() + "://" + name.getAuthority());
    }

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
            throws IOException {
        return wrappedFS.append(arg0, arg1);
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1,
            boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
            throws IOException {
        return wrappedFS.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
        return wrappedFS.delete(arg0, arg1);
    }

    @Override
    public FileStatus getFileStatus(Path arg0) throws IOException {
        return wrappedFS.getFileStatus(arg0);
    }

    @Override
    public URI getUri() {
        return fileSystemUri;
    }

    @Override
    public String getScheme() {
        return "sfs";
    }

    @Override
    public Path getWorkingDirectory() {
        return wrappedFS.getWorkingDirectory();
    }

    @Override
    public FileStatus[] listStatus(Path arg0) throws FileNotFoundException,
            IOException {
        return wrappedFS.listStatus(arg0);
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
        return wrappedFS.mkdirs(arg0, arg1);
    }

    @Override
    public FSDataInputStream open(Path arg0, int arg1) throws IOException {
        return wrappedFS.open(arg0, arg1);
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
        return wrappedFS.rename(arg0, arg1);
    }

    @Override
    public void setWorkingDirectory(Path arg0) {
        wrappedFS.setWorkingDirectory(arg0);
    }
}
