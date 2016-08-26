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
import java.lang.reflect.Method;
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

    public static final String SFS_WRAPPED_FS_FACTORY_CLASS_NAME_KEY = "sfs.wrappedFS.factoryClassName";

    public static final String SFS_WRAPPED_FS_SCHEME = "sfs.wrappedFS.scheme";

    private URI fileSystemUri;

    private FileSystem wrappedFS;

    private String wrappedFSScheme;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        wrappedFSScheme = getConf().get(SFS_WRAPPED_FS_SCHEME, "hdfs");
        URI wrappedFSUri = URI.create(wrappedFSScheme + "://"
                + name.getAuthority());

        String wrappedFSFactoryClassName = getConf().get(
                SFS_WRAPPED_FS_FACTORY_CLASS_NAME_KEY,
                "org.apache.hadoop.fs.FileSystem");

        Class<?> wrappedFSFactoryClass;
        try {
            wrappedFSFactoryClass = Class.forName(wrappedFSFactoryClassName);
        } catch (Exception e) {
            throw new IOException("Error obtaining factory class '"
                    + wrappedFSFactoryClassName + "'", e);
        }

        try {
            // try .get(URI, Configuration) first
            Method getMethod = wrappedFSFactoryClass.getMethod("get",
                    URI.class, Configuration.class);
            wrappedFS = (FileSystem) getMethod.invoke(null, wrappedFSUri,
                    getConf());
        } catch (NoSuchMethodException e) {
            // try .get(URI)
            Method getMethod;
            try {
                getMethod = wrappedFSFactoryClass.getMethod("get", URI.class);
                wrappedFS = (FileSystem) getMethod.invoke(null, wrappedFSUri);
            } catch (NoSuchMethodException e1) {
                throw new IOException(
                        "No appropriate get method found in factory class '"
                                + wrappedFSFactoryClassName + "'", e1);
            } catch (Exception e1) {
                throw new IOException("Error obtaining class for scheme '"
                        + wrappedFSScheme + "' from factory class '"
                        + wrappedFSFactoryClassName + "'", e1);
            }
        } catch (Exception e) {
            throw new IOException("Error obtaining class for scheme "
                    + wrappedFSScheme + " from factory class "
                    + wrappedFSFactoryClassName, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Wrapping file system with scheme '" + wrappedFSScheme
                    + "' as '" + getScheme() + "'.");
            LOG.debug("You can change it by setting '"
                    + SFS_WRAPPED_FS_FACTORY_CLASS_NAME_KEY + "'.");
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
        return replacePathScheme(path, wrappedFSScheme, getScheme());
    }

    private Path unwrapPath(Path path) {
        return replacePathScheme(path, getScheme(), wrappedFSScheme);
    }

    private Path replacePathScheme(Path path, String from, String to) {
        URI pathUri = path.toUri();
        String scheme = pathUri.getScheme();
        if (scheme != null) {
            if (scheme.equalsIgnoreCase(from)) {
                // path has this scheme, replace it with new scheme
                return new Path(UriBuilder.fromUri(pathUri).scheme(to).build());
            } else if (scheme.equalsIgnoreCase(to)) {
                // path already has the correct scheme
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Path '" + path
                            + "' already has the correct scheme '" + to + "'.");
                }
                return path;
            } else {
                // path has wrong scheme
                throw new IllegalArgumentException("Wrong scheme: '" + scheme
                        + "' in path '" + path + "', expected '" + from + "'.");
            }
        } else {
            // path has no scheme, just return it
            return path;
        }
    }
}
