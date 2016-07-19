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
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        return wrappedFS.append(f, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        return wrappedFS.create(f, permission, overwrite, bufferSize,
                replication, blockSize, progress);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        return wrappedFS.delete(f, recursive);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return wrappedFS.getFileStatus(f);
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
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
            IOException {
        return wrappedFS.listStatus(f);
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        return wrappedFS.mkdirs(f, permission);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        return wrappedFS.open(f, bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return wrappedFS.rename(src, dst);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        wrappedFS.setWorkingDirectory(new_dir);
    }
}
