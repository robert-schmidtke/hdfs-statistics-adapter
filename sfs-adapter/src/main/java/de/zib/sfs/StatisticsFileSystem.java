/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.EnumSet;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator;
import de.zib.sfs.instrument.statistics.OperationCategory;
import de.zib.sfs.instrument.statistics.OperationSource;

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
     * The scheme of the wrapped file system.
     */
    public static final String SFS_WRAPPED_FS_SCHEME_KEY = "sfs.wrappedFS.scheme";

    /**
     * r|w|o or any combination of them, indicates which operation categories
     * not to log.
     */
    public static final String SFS_INSTRUMENTATION_SKIP = "sfs.instrumentation.skip";

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

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
     * Flag to track whether this file system is closed already.
     */
    private boolean closed = false;

    /**
     * Flag to track whether this file system is initialized already.
     */
    private boolean initialized = false;

    /**
     * Set of operation categories to skip.
     */
    private boolean skipRead = false, skipWrite = false, skipOther = false;

    @Override
    public synchronized void initialize(URI name, Configuration conf)
            throws IOException {
        if (initialized) {
            LOG.warn("Ignoring attempt to re-initialize file system.");
            return;
        }

        super.initialize(name, conf);
        setConf(conf);

        String hostname = System.getProperty("de.zib.sfs.hostname");
        if (hostname == null) {
            LOG.warn(
                    "'de.zib.sfs.hostname' not set, did the agent start properly?");

            // Obtain hostname, preferably via executing hostname
            Process hostnameProcess = Runtime.getRuntime().exec("hostname");
            try {
                int exitCode = hostnameProcess.waitFor();
                if (exitCode != 0) {
                    LOG.warn("'hostname' returned " + exitCode
                            + ", using $HOSTNAME instead.");
                    hostname = System.getenv("HOSTNAME");
                } else {
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(
                                    hostnameProcess.getInputStream()));

                    StringBuilder hostnameBuilder = new StringBuilder();
                    String line = "";
                    while ((line = reader.readLine()) != null) {
                        hostnameBuilder.append(line);
                    }
                    reader.close();
                    hostname = hostnameBuilder.toString();
                }
            } catch (InterruptedException e) {
                LOG.warn("Error executing 'hostname', using $HOSTNAME instead.",
                        e);
                hostname = System.getenv("HOSTNAME");
            }

            System.setProperty("de.zib.sfs.hostname", hostname);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Running on " + hostname + ".");
        }

        if (System.getProperty("de.zib.sfs.pid") == null) {
            LOG.warn("'de.zib.sfs.pid' not set, did the agent start properly?");

            // use negative random number to indicate it's no real PID
            int pid = -new Random().nextInt(Integer.MAX_VALUE);
            System.setProperty("de.zib.sfs.pid", Integer.toString(pid));
        }

        if (System.getProperty("de.zib.sfs.key") == null) {
            LOG.warn("'de.zib.sfs.key' not set, did the agent start properly?");
            System.setProperty("de.zib.sfs.key", "sfs");
        }

        if (System.getProperty("de.zib.sfs.timeBin.duration") == null) {
            LOG.warn(
                    "'de.zib.sfs.timeBin.duration' not set, did the agent start properly?");
            System.setProperty("de.zib.sfs.timeBin.duration", "1000");
        }

        if (System.getProperty("de.zib.sfs.timeBin.cacheSize") == null) {
            LOG.warn(
                    "'de.zib.sfs.timeBin.cacheSize' not set, did the agent start properly?");
            System.setProperty("de.zib.sfs.timeBin.cacheSize", "30");
        }

        if (System.getProperty("de.zib.sfs.output.directory") == null) {
            LOG.warn(
                    "'de.zib.sfs.output.directory' not set, did the agent start properly?");
            System.setProperty("de.zib.sfs.output.directory", "/tmp");
        }

        LiveOperationStatisticsAggregator.instance.initialize();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initialized file system statistics aggregator.");
        }

        // Obtain the file system class we want to wrap
        String wrappedFSClassName = getConf()
                .get(SFS_WRAPPED_FS_CLASS_NAME_KEY);
        if (wrappedFSClassName == null) {
            throw new RuntimeException(
                    SFS_WRAPPED_FS_CLASS_NAME_KEY + " not specified");
        }
        wrappedFSScheme = getConf().get(SFS_WRAPPED_FS_SCHEME_KEY);
        if (wrappedFSScheme == null) {
            throw new RuntimeException(
                    SFS_WRAPPED_FS_SCHEME_KEY + " not specified");
        }

        Class<?> wrappedFSClass;
        try {
            wrappedFSClass = Class.forName(wrappedFSClassName);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error obtaining class '" + wrappedFSClassName + "'", e);
        }

        // Figure out what kind of file system we are wrapping.
        if (wrappedFSClassName.startsWith("org.apache.hadoop")
                || wrappedFSClassName
                        .startsWith("org.xtreemfs.common.clients.hadoop")) {
            try {
                // Wrap Hadoop file system directly.
                wrappedFS = wrappedFSClass.asSubclass(FileSystem.class)
                        .newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Error instantiating Hadoop class '"
                        + wrappedFSClassName + "'", e);
            }
        } else {
            throw new RuntimeException("Unsupported file system class '"
                    + wrappedFSClassName + "'");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Wrapping file system '" + wrappedFS.getClass().getName()
                    + "' with scheme '" + wrappedFSScheme + "' as '"
                    + getScheme() + "'.");
            LOG.debug("You can change it by setting '"
                    + SFS_WRAPPED_FS_CLASS_NAME_KEY + "'.");
        }

        if (name.getAuthority() != null) {
            fileSystemUri = URI
                    .create(getScheme() + "://" + name.getAuthority() + "/");
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

        // Add shutdown hook that closes this file system
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Running shutdown hook.");
                }

                try {
                    StatisticsFileSystem.this.close(true);
                } catch (IOException e) {
                    LOG.error("Could not close file system.", e);
                }
            }
        });

        String instrumentationSkip = getConf().get(SFS_INSTRUMENTATION_SKIP);
        if (instrumentationSkip != null) {
            skipRead = instrumentationSkip.contains("r");
            skipWrite = instrumentationSkip.contains("w");
            skipOther = instrumentationSkip.contains("o");
        }

        initialized = true;
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer,
            Credentials credentials) throws IOException {
        return wrappedFS.addDelegationTokens(renewer, credentials);
    }

    @Override
    public FSDataOutputStream append(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.append(unwrappedPath),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.append(unwrappedPath);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.append(unwrappedPath, bufferSize),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.append(unwrappedPath, bufferSize);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.append(unwrappedPath, bufferSize, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.append(unwrappedPath, bufferSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public boolean cancelDeleteOnExit(Path f) {
        return wrappedFS.cancelDeleteOnExit(unwrapPath(f));
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    private synchronized final void close(boolean fromShutdownHook)
            throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file system.");
        }

        if (closed) {
            LOG.warn("Ignoring attempt to re-close file system.");
            return;
        }

        // If called from a shutdown hook, org.apache.hadoop.fs.FileSystem will
        // be closed during its own shutdown hook, so avoid deadlock here by
        // only closing wrappedFS and super when explicitly closed.
        if (!fromShutdownHook) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing wrapped file system.");
            }
            wrappedFS.close();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closed wrapped file system.");
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing parent file system.");
            }
            super.close();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closed parent file system.");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Flushing statistics.");
        }
        LiveOperationStatisticsAggregator.instance.flush();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Flushed statistics.");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Closed file system.");
        }
        closed = true;
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException {
        wrappedFS.completeLocalOutput(unwrapPath(fsOutputFile),
                unwrapPath(tmpLocalFile));
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        Path[] unwrappedPsrcs = new Path[psrcs.length];
        for (int i = 0; i < psrcs.length; ++i) {
            unwrappedPsrcs[i] = unwrapPath(psrcs[i]);
        }
        wrappedFS.concat(unwrapPath(trg), unwrappedPsrcs);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
            Path dst) throws IOException {
        wrappedFS.copyFromLocalFile(delSrc, overwrite, unwrapPath(src),
                unwrapPath(dst));
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite,
            Path[] srcs, Path dst) throws IOException {
        Path[] unwrappedSrcs = new Path[srcs.length];
        for (int i = 0; i < srcs.length; ++i) {
            unwrappedSrcs[i] = unwrapPath(srcs[i]);
        }
        wrappedFS.copyFromLocalFile(delSrc, overwrite, srcs, unwrapPath(dst));
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        wrappedFS.copyFromLocalFile(delSrc, unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst) throws IOException {
        wrappedFS.copyFromLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        wrappedFS.copyToLocalFile(delSrc, unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst,
            boolean useRawLocalFileSystem) throws IOException {
        wrappedFS.copyToLocalFile(delSrc, unwrapPath(src), unwrapPath(dst),
                useRawLocalFileSystem);
    }

    @Override
    public void copyToLocalFile(Path src, Path dst) throws IOException {
        wrappedFS.copyToLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, overwrite),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, overwrite);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, overwrite, bufferSize),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, overwrite, bufferSize);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                            progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                    progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
            short replication, long blockSize) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                            replication, blockSize),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                    replication, blockSize);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
            short replication, long blockSize, Progressable progress)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                            replication, blockSize, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                    replication, blockSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, permission, overwrite,
                            bufferSize, replication, blockSize, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, permission, overwrite,
                    bufferSize, replication, blockSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, permission, flags,
                            bufferSize, replication, blockSize, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, permission, flags,
                    bufferSize, replication, blockSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            EnumSet<CreateFlag> flags, int bufferSize, short replication,
            long blockSize, Progressable progress, ChecksumOpt checksumOpt)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, permission, flags,
                            bufferSize, replication, blockSize, progress,
                            checksumOpt),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, permission, flags,
                    bufferSize, replication, blockSize, progress, checksumOpt);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, short replication)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, replication),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, replication);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, short replication,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.create(unwrappedPath, replication, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.create(unwrappedPath, replication, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public boolean createNewFile(Path f) throws IOException {
        return wrappedFS.createNewFile(unwrapPath(f));
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite,
            int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.createNonRecursive(unwrappedPath, overwrite,
                            bufferSize, replication, blockSize, progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.createNonRecursive(unwrappedPath, overwrite,
                    bufferSize, replication, blockSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f,
            FsPermission permission, boolean overwrite, int bufferSize,
            short replication, long blockSize, Progressable progress)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.createNonRecursive(unwrappedPath, permission,
                            overwrite, bufferSize, replication, blockSize,
                            progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.createNonRecursive(unwrappedPath, permission,
                    overwrite, bufferSize, replication, blockSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f,
            FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize,
            short replication, long blockSize, Progressable progress)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    wrappedFS.createNonRecursive(unwrappedPath, permission,
                            flags, bufferSize, replication, blockSize,
                            progress),
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = wrappedFS.createNonRecursive(unwrappedPath, permission,
                    flags, bufferSize, replication, blockSize, progress);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException {
        return wrapPath(
                wrappedFS.createSnapshot(unwrapPath(path), snapshotName));
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException,
            FileNotFoundException, ParentNotDirectoryException,
            UnsupportedFileSystemException, IOException {
        wrappedFS.createSymlink(unwrapPath(target), unwrapPath(link),
                createParent);
    }

    @Override
    @Deprecated
    public boolean delete(Path f) throws IOException {
        return wrappedFS.delete(unwrapPath(f));
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        boolean result = wrappedFS.delete(unwrappedPath, recursive);
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return result;
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
        return wrappedFS.deleteOnExit(unwrapPath(f));
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException {
        wrappedFS.deleteSnapshot(unwrapPath(path), snapshotName);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return wrappedFS.exists(unwrapPath(path));
    }

    @Override
    @Deprecated
    public long getBlockSize(Path f) throws IOException {
        return wrappedFS.getBlockSize(unwrapPath(f));
    }

    @Override
    public String getCanonicalServiceName() {
        return wrappedFS.getCanonicalServiceName();
    }

    @Override
    public FileSystem[] getChildFileSystems() {
        return wrappedFS.getChildFileSystems();
    }

    @Override
    public ContentSummary getContentSummary(Path path) throws IOException {
        return wrappedFS.getContentSummary(unwrapPath(path));
    }

    @Override
    @Deprecated
    public long getDefaultBlockSize() {
        return wrappedFS.getDefaultBlockSize();
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return wrappedFS.getDefaultBlockSize(unwrapPath(f));
    }

    @Override
    @Deprecated
    public short getDefaultReplication() {
        return wrappedFS.getDefaultReplication();
    }

    @Override
    public short getDefaultReplication(Path path) {
        return wrappedFS.getDefaultReplication(unwrapPath(path));
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        return wrappedFS.getDelegationToken(renewer);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
            long len) throws IOException {
        long startTime = System.currentTimeMillis();
        Path path = file.getPath();
        file.setPath(unwrapPath(path));
        BlockLocation[] blockLocations = wrappedFS.getFileBlockLocations(file,
                start, len);
        file.setPath(path);
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return blockLocations;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path path = unwrapPath(p);
        BlockLocation[] blockLocations = wrappedFS.getFileBlockLocations(path,
                start, len);
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return blockLocations;
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        return wrappedFS.getFileChecksum(unwrapPath(f));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws AccessControlException,
            FileNotFoundException, UnsupportedFileSystemException, IOException {
        FileStatus fileStatus = wrappedFS.getFileLinkStatus(unwrapPath(f));
        fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                f.toUri().getAuthority()));
        return fileStatus;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FileStatus fileStatus = wrappedFS.getFileStatus(unwrappedPath);
        fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                f.toUri().getAuthority()));
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return fileStatus;
    }

    @Override
    public Path getHomeDirectory() {
        return wrapPath(wrappedFS.getHomeDirectory());
    }

    @Override
    @Deprecated
    public long getLength(Path f) throws IOException {
        return wrappedFS.getLength(unwrapPath(f));
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        return wrapPath(wrappedFS.getLinkTarget(unwrapPath(f)));
    }

    @Override
    @Deprecated
    public String getName() {
        return wrappedFS.getName();
    }

    @Override
    @Deprecated
    public short getReplication(Path src) throws IOException {
        return wrappedFS.getReplication(unwrapPath(src));
    }

    @Override
    public String getScheme() {
        return "sfs";
    }

    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults() throws IOException {
        return wrappedFS.getServerDefaults();
    }

    @Override
    public FsServerDefaults getServerDefaults(Path p) throws IOException {
        return wrappedFS.getServerDefaults(unwrapPath(p));
    }

    @Override
    public FsStatus getStatus() throws IOException {
        return wrappedFS.getStatus();
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return wrappedFS.getStatus(unwrapPath(p));
    }

    @Override
    public URI getUri() {
        return fileSystemUri;
    }

    @Override
    public long getUsed() throws IOException {
        return wrappedFS.getUsed();
    }

    @Override
    public Path getWorkingDirectory() {
        Path f = wrappedFS.getWorkingDirectory();
        Path wrappedWorkingDirectory = setAuthority(wrapPath(f),
                fileSystemUri.getAuthority());
        return wrappedWorkingDirectory;
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        FileStatus[] fileStatuses = wrappedFS
                .globStatus(unwrapPath(pathPattern));
        for (FileStatus fileStatus : fileStatuses) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                    pathPattern.toUri().getAuthority()));
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
            throws IOException {
        PathFilter wrappedFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return filter.accept(unwrapPath(path));
            }
        };

        FileStatus[] fileStatuses = wrappedFS
                .globStatus(unwrapPath(pathPattern), wrappedFilter);
        for (FileStatus fileStatus : fileStatuses) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                    pathPattern.toUri().getAuthority()));
        }
        return fileStatuses;
    }

    @Override
    public boolean isDirectory(Path path) throws IOException {
        return wrappedFS.isDirectory(unwrapPath(path));
    }

    @Override
    public boolean isFile(Path path) throws IOException {
        return wrappedFS.isFile(unwrapPath(path));
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException {
        final RemoteIterator<Path> it = wrappedFS
                .listCorruptFileBlocks(unwrapPath(path));
        return new RemoteIterator<Path>() {
            @Override
            public boolean hasNext() throws IOException {
                return it.hasNext();
            }

            @Override
            public Path next() throws IOException {
                return wrapPath(it.next());
            }
        };
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f,
            boolean recursive) throws FileNotFoundException, IOException {
        final RemoteIterator<LocatedFileStatus> it = wrappedFS
                .listFiles(unwrapPath(f), recursive);
        return new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return it.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                LocatedFileStatus fileStatus = it.next();
                fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                        f.toUri().getAuthority()));
                return fileStatus;
            }
        };
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws FileNotFoundException, IOException {
        final RemoteIterator<LocatedFileStatus> it = wrappedFS
                .listLocatedStatus(f);
        return new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return it.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                LocatedFileStatus fileStatus = it.next();
                fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                        f.toUri().getAuthority()));
                return fileStatus;
            }
        };
    }

    @Override
    public FileStatus[] listStatus(Path f)
            throws FileNotFoundException, IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FileStatus[] fileStatuses = wrappedFS.listStatus(unwrappedPath);
        for (FileStatus fileStatus : fileStatuses) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                    f.toUri().getAuthority()));
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        PathFilter wrappedFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return filter.accept(unwrapPath(path));
            }
        };

        FileStatus[] fileStatuses = wrappedFS.listStatus(unwrappedPath,
                wrappedFilter);
        for (FileStatus fileStatus : fileStatuses) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                    f.toUri().getAuthority()));
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] listStatus(Path[] files)
            throws FileNotFoundException, IOException {
        Path[] unwrappedFiles = new Path[files.length];
        for (int i = 0; i < files.length; ++i) {
            unwrappedFiles[i] = unwrapPath(files[i]);
        }

        FileStatus[] fileStatuses = wrappedFS.listStatus(unwrappedFiles);
        for (int i = 0; i < fileStatuses.length; ++i) {
            fileStatuses[i]
                    .setPath(setAuthority(wrapPath(fileStatuses[i].getPath()),
                            files[i].toUri().getAuthority()));
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] listStatus(Path[] path, PathFilter filter)
            throws FileNotFoundException, IOException {
        Path[] unwrappedPath = new Path[path.length];
        for (int i = 0; i < path.length; ++i) {
            unwrappedPath[i] = unwrapPath(path[i]);
        }

        PathFilter wrappedFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return filter.accept(unwrapPath(path));
            }
        };

        FileStatus[] fileStatuses = wrappedFS.listStatus(unwrappedPath,
                wrappedFilter);
        for (int i = 0; i < fileStatuses.length; ++i) {
            fileStatuses[i]
                    .setPath(setAuthority(wrapPath(fileStatuses[i].getPath()),
                            path[i].toUri().getAuthority()));
        }
        return fileStatuses;
    }

    @Override
    public Path makeQualified(Path path) {
        return wrapPath(wrappedFS.makeQualified(unwrapPath(path)));
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return wrappedFS.mkdirs(unwrapPath(f));
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        boolean result = wrappedFS.mkdirs(unwrappedPath, permission);
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return result;
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
        wrappedFS.moveFromLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
        Path[] unwrappedSrcs = new Path[srcs.length];
        for (int i = 0; i < srcs.length; ++i) {
            unwrappedSrcs[i] = unwrapPath(srcs[i]);
        }
        wrappedFS.moveFromLocalFile(unwrappedSrcs, unwrapPath(dst));
    }

    @Override
    public void moveToLocalFile(Path src, Path dst) throws IOException {
        wrappedFS.moveToLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataInputStream stream;
        if (!skipRead) {
            stream = new FSDataInputStream(
                    new WrappedFSDataInputStream(wrappedFS.open(unwrappedPath),
                            LiveOperationStatisticsAggregator.instance));
        } else {
            stream = wrappedFS.open(unwrappedPath);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataInputStream stream;
        if (!skipRead) {
            stream = new FSDataInputStream(new WrappedFSDataInputStream(
                    wrappedFS.open(unwrappedPath, bufferSize),
                    LiveOperationStatisticsAggregator.instance));
        } else {
            stream = wrappedFS.open(unwrappedPath, bufferSize);
        }
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return stream;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedSrc = unwrapPath(src);
        Path unwrappedDst = unwrapPath(dst);
        boolean result = wrappedFS.rename(unwrappedSrc, unwrappedDst);
        if (!skipOther) {
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis());
        }
        return result;
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
            String snapshotNewName) throws IOException {
        wrappedFS.renameSnapshot(unwrapPath(path), snapshotOldName,
                snapshotNewName);
    }

    @Override
    public Path resolvePath(Path p) throws IOException {
        return wrapPath(wrappedFS.resolvePath(unwrapPath(p)));
    }

    @Override
    public void setOwner(Path p, String username, String groupname)
            throws IOException {
        wrappedFS.setOwner(unwrapPath(p), username, groupname);
    }

    @Override
    public void setPermission(Path p, FsPermission permission)
            throws IOException {
        wrappedFS.setPermission(unwrapPath(p), permission);
    }

    @Override
    public boolean setReplication(Path src, short replication)
            throws IOException {
        return wrappedFS.setReplication(unwrapPath(src), replication);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        wrappedFS.setTimes(unwrapPath(p), mtime, atime);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
        wrappedFS.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        Path unwrappedPath = unwrapPath(new_dir);
        wrappedFS.setWorkingDirectory(unwrappedPath);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum) {
        wrappedFS.setWriteChecksum(writeChecksum);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException {
        return wrapPath(wrappedFS.startLocalOutput(unwrapPath(fsOutputFile),
                unwrapPath(tmpLocalFile)));
    }

    @Override
    public boolean supportsSymlinks() {
        return wrappedFS.supportsSymlinks();
    }

    // Helper methods.

    private URI replaceUriScheme(URI uri, String from, String to) {
        // TODO add cache for replaced URIs? possibly useful for scenarions with
        // many metadata operations.

        String scheme = uri.getScheme();
        if (scheme != null) {
            if (scheme.equalsIgnoreCase(from)) {
                // uri has this scheme, replace it with new scheme

                // re-create the URI from scratch to avoid escaping of wanted
                // illegal characters
                StringBuilder buffer = new StringBuilder();

                buffer.append(to).append(":");

                String authority = uri.getAuthority();
                if (authority != null) {
                    buffer.append("//").append(authority);
                }

                String path = uri.getPath();
                if (path != null) {
                    buffer.append(path);
                }

                String fragment = uri.getFragment();
                if (fragment != null) {
                    buffer.append("#").append(fragment);
                }

                return URI.create(buffer.toString());
            } else if (scheme.equalsIgnoreCase(to)) {
                // uri already has the correct scheme
                if (LOG.isDebugEnabled()) {
                    LOG.debug("URI '" + uri
                            + "' already has the correct scheme '" + to + "'.");
                }
                return uri;
            } else {
                // uri has wrong scheme, just return it
                return uri;
            }
        } else {
            // uri has no scheme, just return it
            return uri;
        }
    }

    private Path setAuthority(Path path, String authority) {
        if (authority != null) {
            URI pathUri = path.toUri();
            String query = pathUri.getQuery();
            String fragment = pathUri.getFragment();
            return new Path(URI
                    .create(pathUri.getScheme() + "://" + authority + "/"
                            + pathUri.getPath()
                            + (query != null ? ("?" + query) : ""))
                    + (fragment != null ? ("#" + fragment) : ""));
        } else {
            return path;
        }
    }

    private Path unwrapPath(Path path) {
        return new Path(
                replaceUriScheme(path.toUri(), getScheme(), wrappedFSScheme));
    }

    private Path wrapPath(Path path) {
        return new Path(
                replaceUriScheme(path.toUri(), wrappedFSScheme, getScheme()));
    }
}
