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

import javax.ws.rs.core.UriBuilder;

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

    private String wrappedScheme;

    private FileSystem wrappedFS;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        wrappedScheme = getConf().get("sfs.wrappedFS", "hdfs");
        wrappedFS = get(
                URI.create(wrappedScheme + "://" + name.getAuthority()),
                getConf());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Wrapping file system with scheme '" + wrappedFS + "'.");
        }

        fileSystemUri = URI.create(getScheme() + "://" + name.getAuthority());
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        Path unwrappedPath = unwrapPath(f);
        return wrappedFS.append(unwrappedPath, bufferSize, progress);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        Path unwrappedPath = unwrapPath(f);
        return wrappedFS.create(unwrappedPath, permission, overwrite,
                bufferSize, replication, blockSize, progress);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        Path unwrappedPath = unwrapPath(f);
        return wrappedFS.delete(unwrappedPath, recursive);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        Path unwrappedPath = unwrapPath(f);
        FileStatus fileStatus = wrappedFS.getFileStatus(unwrappedPath);
        fileStatus.setPath(wrapPath(fileStatus.getPath()));
        return fileStatus;
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
        Path f = wrappedFS.getWorkingDirectory();
        Path wrappedWorkingDirectory = wrapPath(f);
        return wrappedWorkingDirectory;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
            IOException {
        Path unwrappedPath = unwrapPath(f);
        FileStatus[] fileStatuses = wrappedFS.listStatus(unwrappedPath);
        for (FileStatus fileStatus : fileStatuses) {
            fileStatus.setPath(wrapPath(fileStatus.getPath()));
        }
        return fileStatuses;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        Path unwrappedPath = unwrapPath(f);
        return wrappedFS.mkdirs(unwrappedPath, permission);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        Path unwrappedPath = unwrapPath(f);
        return wrappedFS.open(unwrappedPath, bufferSize);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        Path unwrappedSrc = unwrapPath(src);
        Path unwrappedDst = unwrapPath(dst);
        return wrappedFS.rename(unwrappedSrc, unwrappedDst);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        Path unwrappedPath = unwrapPath(new_dir);
        wrappedFS.setWorkingDirectory(unwrappedPath);
    }

    // Helper methods.

    private Path wrapPath(Path path) {
        return replacePathScheme(path, wrappedScheme, getScheme());
    }

    private Path unwrapPath(Path path) {
        return replacePathScheme(path, getScheme(), wrappedScheme);
    }

    private Path replacePathScheme(Path path, String from, String to) {
        URI pathUri = path.toUri();
        String scheme = pathUri.getScheme();
        if (scheme != null) {
            if (scheme.equalsIgnoreCase(from)) {
                // path has this scheme, replace it with new scheme
                return new Path(UriBuilder.fromUri(pathUri).scheme(to).build());
            } else {
                // path has wrong scheme
                throw new IllegalArgumentException("Wrong scheme: " + scheme
                        + ", expected " + from);
            }
        } else {
            // path has no scheme, just return it
            return path;
        }
    }
}
