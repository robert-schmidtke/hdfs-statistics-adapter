/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
     * Location of the log file on each host.
     */
    public static final String SFS_LOG_FILE_NAME_KEY = "sfs.logFileName";

    /**
     * Directory to copy the host log file to during
     * {@link StatisticsFileSystem#close()}.
     */
    public static final String SFS_TARGET_LOG_FILE_DIRECTORY_KEY = "sfs.targetLogFileDirectory";

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

    /**
     * The host we're running on.
     */
    private String hostname;

    /**
     * The actual logger for file system calls.
     */
    private Logger fsLogger;

    /**
     * The log file to log all events to.
     */
    private File logFile;

    /**
     * Path to copy the generated log file to during
     * {@link StatisticsFileSystem#close()}.
     */
    private String targetLogFileDirectory;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        // Obtain hostname, preferably vis executing hostname
        Process hostnameProcess = Runtime.getRuntime().exec("hostname");
        try {
            int exitCode = hostnameProcess.waitFor();
            if (exitCode != 0) {
                LOG.warn("'hostname' returned " + exitCode
                        + ", using $HOSTNAME instead.");
                hostname = System.getenv("HOSTNAME");
            } else {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(hostnameProcess.getInputStream()));

                StringBuilder hostnameBuilder = new StringBuilder();
                String line = "";
                while ((line = reader.readLine()) != null) {
                    hostnameBuilder.append(line);
                }
                reader.close();
                hostname = hostnameBuilder.toString();
            }
        } catch (InterruptedException e) {
            LOG.warn("Error executing 'hostname', using $HOSTNAME instead.", e);
            hostname = System.getenv("HOSTNAME");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Running on " + hostname + ".");
        }

        // Set up the logger for file system events
        String logFileName = getConf().get(SFS_LOG_FILE_NAME_KEY);
        if (logFileName == null) {
            throw new RuntimeException(SFS_LOG_FILE_NAME_KEY + " not specified");
        }

        logFile = new File(logFileName);

        if (!logFile.getParentFile().exists()) {
            if (!logFile.getParentFile().mkdirs()) {
                throw new RuntimeException(
                        "Could not create log file directories: "
                                + logFile.getParentFile().getAbsolutePath());
            }
        }

        System.setProperty("de.zib.sfs.asyncLogFileName", logFileName);
        fsLogger = LogManager.getLogger("de.zib.sfs.AsyncLogger");

        // Get the target log file directory
        targetLogFileDirectory = getConf().get(
                SFS_TARGET_LOG_FILE_DIRECTORY_KEY);

        // Obtain the file system class we want to wrap
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
    public void close() throws IOException {
        super.close();

        if (targetLogFileDirectory != null) {
            File targetLogFileDirectoryFile = new File(targetLogFileDirectory);
            if (!targetLogFileDirectoryFile.exists()) {
                if (!targetLogFileDirectoryFile.mkdirs()) {
                    // Just warn, maybe some other process has just created the
                    // shared directory
                    LOG.warn("Could not create target log file directory "
                            + targetLogFileDirectory);
                }
            }
            Files.copy(Paths.get(logFile.getAbsolutePath()), Paths.get(
                    targetLogFileDirectoryFile.getAbsolutePath(),
                    logFile.getName() + "." + hostname));
        }
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
        fsLogger.info("open(" + f + "," + bufferSize + ")");
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
