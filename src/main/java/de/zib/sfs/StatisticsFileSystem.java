/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import de.zib.sfs.flink.WrappedFlinkFileSystem;

/**
 * Implements the Hadoop {@link org.apache.hadoop.fs.FileSystem} interface as it
 * is used in Hadoop and Flink.
 * 
 * @author robert
 *
 */
public class StatisticsFileSystem extends FileSystem {

    /**
     * The fully qualified class name of the file system implementation to wrap.
     * Must be a subclass of {@link org.apache.hadoop.fs.FileSystem} or
     * {@link org.apache.flink.core.fs.FileSystem}.
     */
    public static final String SFS_WRAPPED_FS_CLASS_NAME_KEY = "sfs.wrappedFS.className";

    /**
     * The URI of this file system, as sfs:// plus the authority of the wrapped
     * file system.
     */
    private URI fileSystemUri;

    /**
     * The wrapped file system implementation.
     */
    private FileSystem wrappedFS;

    /**
     * The scheme of the wrapped file system.
     */
    private String wrappedFSScheme;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        String wrappedFSClassName = getConf()
                .get(SFS_WRAPPED_FS_CLASS_NAME_KEY);
        if (wrappedFSClassName == null) {
            throw new RuntimeException(SFS_WRAPPED_FS_CLASS_NAME_KEY
                    + " not specified");
        }

        Class<?> wrappedFSClass;
        try {
            wrappedFSClass = Class.forName(wrappedFSClassName);
        } catch (Exception e) {
            throw new RuntimeException("Error obtaining class '"
                    + wrappedFSClassName + "'", e);
        }

        // Figure out what kind of file system we are wrapping.
        if (wrappedFSClassName.startsWith("org.apache.hadoop")) {
            try {
                // Wrap Hadoop file system directly.
                wrappedFS = wrappedFSClass.asSubclass(FileSystem.class)
                        .newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Error instantiating Hadoop class '"
                        + wrappedFSClassName + "'", e);
            }
        } else if (wrappedFSClassName.startsWith("org.apache.flink")) {
            try {
                // Wrap Flink's file system as Hadoop first.
                wrappedFS = new WrappedFlinkFileSystem(wrappedFSClass
                        .asSubclass(org.apache.flink.core.fs.FileSystem.class)
                        .newInstance());
            } catch (Exception e) {
                throw new RuntimeException("Error instantiating Flink class '"
                        + wrappedFSClassName + "'", e);
            }
        } else {
            throw new RuntimeException("Unsupported file system class '"
                    + wrappedFSClassName + "'");
        }

        try {
            wrappedFSScheme = wrappedFS.getScheme();
        } catch (UnsupportedOperationException e) {
            // Not all file systems implement getScheme().
            wrappedFSScheme = wrappedFS.getUri().getScheme();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Wrapping file system '" + wrappedFS.getClass().getName()
                    + "' with scheme '" + wrappedFSScheme + "' as '"
                    + getScheme() + "'.");
            LOG.debug("You can change it by setting '"
                    + SFS_WRAPPED_FS_CLASS_NAME_KEY + "'.");
        }

        if (name.getAuthority() != null) {
            fileSystemUri = URI.create(getScheme() + "://"
                    + name.getAuthority() + "/");
        } else {
            fileSystemUri = URI.create(getScheme() + ":///");
        }

        // Finally initialize the wrapped file system with the unwrapped name.
        URI wrappedFSUri = replaceUriScheme(name, getScheme(), wrappedFSScheme);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing wrapped file system with URI '"
                    + wrappedFSUri + "'.");
        }
        wrappedFS.initialize(wrappedFSUri, conf);
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
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
            long len) throws IOException {
        FileStatus unwrappedFile = file;
        unwrappedFile.setPath(unwrapPath(file.getPath()));
        return wrappedFS.getFileBlockLocations(unwrappedFile, start, len);
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
        return new Path(replaceUriScheme(path.toUri(), wrappedFSScheme,
                getScheme()));
    }

    private Path unwrapPath(Path path) {
        return new Path(replaceUriScheme(path.toUri(), getScheme(),
                wrappedFSScheme));
    }

    private URI replaceUriScheme(URI uri, String from, String to) {
        String scheme = uri.getScheme();
        if (scheme != null) {
            if (scheme.equalsIgnoreCase(from)) {
                // uri has this scheme, replace it with new scheme
                return UriBuilder.fromUri(uri).scheme(to).build();
            } else if (scheme.equalsIgnoreCase(to)) {
                // uri already has the correct scheme
                if (LOG.isDebugEnabled()) {
                    LOG.debug("URI '" + uri
                            + "' already has the correct scheme '" + to + "'.");
                }
                return uri;
            } else {
                // uri has wrong scheme
                throw new IllegalArgumentException("Wrong scheme: '" + scheme
                        + "' in URI '" + uri + "', expected '" + from + "'.");
            }
        } else {
            // uri has no scheme, just return it
            return uri;
        }
    }
}
