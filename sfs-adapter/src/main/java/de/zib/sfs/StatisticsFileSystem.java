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
import de.zib.sfs.instrument.statistics.LiveOperationStatisticsAggregator.OutputFormat;
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
     * Must be a subclass of {@link org.apache.hadoop.fs.FileSystem}.
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
    public static final String SFS_INSTRUMENTATION_SKIP_KEY = "sfs.instrumentation.skip";

    /**
     * Path to a directory.
     */
    public static final String SFS_OUTPUT_DIRECTORY_KEY = "sfs.output.directory";

    /**
     * CSV, FB, BB.
     */
    public static final String SFS_OUTPUT_FORMAT_KEY = "sfs.output.format";

    /**
     * Set to true if instrumentation should be done on per-file basis instead
     * of globally.
     */
    public static final String SFS_TRACE_FDS_KEY = "sfs.traceFds";

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
        if (this.initialized) {
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
                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(
                                    hostnameProcess.getInputStream()))) {

                        StringBuilder hostnameBuilder = new StringBuilder();
                        String line = "";
                        while ((line = reader.readLine()) != null) {
                            hostnameBuilder.append(line);
                        }
                        hostname = hostnameBuilder.toString();
                    }
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
            System.setProperty("de.zib.sfs.output.directory",
                    getConf().get(SFS_OUTPUT_DIRECTORY_KEY, "/tmp"));
        }

        if (System.getProperty("de.zib.sfs.output.format") == null) {
            LOG.warn(
                    "'de.zib.sfs.output.format' not set, did the agent start properly?");
            OutputFormat outputFormat = OutputFormat.valueOf(
                    getConf().get(SFS_OUTPUT_FORMAT_KEY, OutputFormat.BB.name())
                            .toUpperCase());
            System.setProperty("de.zib.sfs.output.format", outputFormat.name());
        }

        if (System.getProperty("de.zib.sfs.traceFds") == null) {
            LOG.warn(
                    "'de.zib.sfs.traceFds' not set, did the agent start properly?");
            System.setProperty("de.zib.sfs.traceFds",
                    getConf().getBoolean(SFS_TRACE_FDS_KEY, false) ? "true"
                            : "false");
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
        this.wrappedFSScheme = getConf().get(SFS_WRAPPED_FS_SCHEME_KEY);
        if (this.wrappedFSScheme == null) {
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
                this.wrappedFS = wrappedFSClass.asSubclass(FileSystem.class)
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
            LOG.debug("Wrapping file system '"
                    + this.wrappedFS.getClass().getName() + "' with scheme '"
                    + this.wrappedFSScheme + "' as '" + getScheme() + "'.");
            LOG.debug("You can change it by setting '"
                    + SFS_WRAPPED_FS_CLASS_NAME_KEY + "'.");
        }

        if (name.getAuthority() != null) {
            this.fileSystemUri = URI
                    .create(getScheme() + "://" + name.getAuthority() + "/");
        } else {
            this.fileSystemUri = URI.create(getScheme() + ":///");
        }

        // Finally initialize the wrapped file system with the unwrapped name.
        URI wrappedFSUri = replaceUriScheme(name, getScheme(),
                this.wrappedFSScheme);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initializing wrapped file system with URI '"
                    + wrappedFSUri + "'.");
        }
        this.wrappedFS.initialize(wrappedFSUri, conf);

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

        String instrumentationSkip = getConf()
                .get(SFS_INSTRUMENTATION_SKIP_KEY);
        if (instrumentationSkip != null) {
            this.skipRead = instrumentationSkip.contains("r");
            this.skipWrite = instrumentationSkip.contains("w");
            this.skipOther = instrumentationSkip.contains("o");
        }

        this.initialized = true;
    }

    @Override
    public Token<?>[] addDelegationTokens(String renewer,
            Credentials credentials) throws IOException {
        return this.wrappedFS.addDelegationTokens(renewer, credentials);
    }

    @Override
    public FSDataOutputStream append(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.append(unwrappedPath), f,
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.append(unwrappedPath);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.append(unwrappedPath, bufferSize), f,
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.append(unwrappedPath, bufferSize);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.append(unwrappedPath, bufferSize, progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.append(unwrappedPath, bufferSize, progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public boolean cancelDeleteOnExit(Path f) {
        return this.wrappedFS.cancelDeleteOnExit(unwrapPath(f));
    }

    @Override
    public void close() throws IOException {
        close(false);
    }

    synchronized final void close(boolean fromShutdownHook) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing file system.");
        }

        if (this.closed) {
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
            this.wrappedFS.close();
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
            LOG.debug("Closed file system.");
        }
        this.closed = true;
    }

    @Override
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException {
        this.wrappedFS.completeLocalOutput(unwrapPath(fsOutputFile),
                unwrapPath(tmpLocalFile));
    }

    @Override
    public void concat(Path trg, Path[] psrcs) throws IOException {
        Path[] unwrappedPsrcs = new Path[psrcs.length];
        for (int i = 0; i < psrcs.length; ++i) {
            unwrappedPsrcs[i] = unwrapPath(psrcs[i]);
        }
        this.wrappedFS.concat(unwrapPath(trg), unwrappedPsrcs);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src,
            Path dst) throws IOException {
        this.wrappedFS.copyFromLocalFile(delSrc, overwrite, unwrapPath(src),
                unwrapPath(dst));
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, boolean overwrite,
            Path[] srcs, Path dst) throws IOException {
        Path[] unwrappedSrcs = new Path[srcs.length];
        for (int i = 0; i < srcs.length; ++i) {
            unwrappedSrcs[i] = unwrapPath(srcs[i]);
        }
        this.wrappedFS.copyFromLocalFile(delSrc, overwrite, srcs,
                unwrapPath(dst));
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        this.wrappedFS.copyFromLocalFile(delSrc, unwrapPath(src),
                unwrapPath(dst));
    }

    @Override
    public void copyFromLocalFile(Path src, Path dst) throws IOException {
        this.wrappedFS.copyFromLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        this.wrappedFS.copyToLocalFile(delSrc, unwrapPath(src),
                unwrapPath(dst));
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst,
            boolean useRawLocalFileSystem) throws IOException {
        this.wrappedFS.copyToLocalFile(delSrc, unwrapPath(src), unwrapPath(dst),
                useRawLocalFileSystem);
    }

    @Override
    public void copyToLocalFile(Path src, Path dst) throws IOException {
        this.wrappedFS.copyToLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public FSDataOutputStream create(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath), f,
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, overwrite), f,
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, overwrite);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, overwrite, bufferSize),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, overwrite,
                    bufferSize);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                            progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                    progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
            short replication, long blockSize) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                            replication, blockSize),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                    replication, blockSize);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
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
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                            replication, blockSize, progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, overwrite, bufferSize,
                    replication, blockSize, progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
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
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, permission, overwrite,
                            bufferSize, replication, blockSize, progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, permission, overwrite,
                    bufferSize, replication, blockSize, progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
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
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, permission, flags,
                            bufferSize, replication, blockSize, progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, permission, flags,
                    bufferSize, replication, blockSize, progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
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
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, permission, flags,
                            bufferSize, replication, blockSize, progress,
                            checksumOpt),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, permission, flags,
                    bufferSize, replication, blockSize, progress, checksumOpt);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, Progressable progress)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, progress), f,
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, short replication)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, replication), f,
                    LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, replication);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataOutputStream create(Path f, short replication,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.create(unwrappedPath, replication, progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.create(unwrappedPath, replication,
                    progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public boolean createNewFile(Path f) throws IOException {
        return this.wrappedFS.createNewFile(unwrapPath(f));
    }

    @Override
    @Deprecated
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite,
            int bufferSize, short replication, long blockSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream;
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.createNonRecursive(unwrappedPath, overwrite,
                            bufferSize, replication, blockSize, progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.createNonRecursive(unwrappedPath, overwrite,
                    bufferSize, replication, blockSize, progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
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
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.createNonRecursive(unwrappedPath, permission,
                            overwrite, bufferSize, replication, blockSize,
                            progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.createNonRecursive(unwrappedPath,
                    permission, overwrite, bufferSize, replication, blockSize,
                    progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
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
        if (!this.skipWrite) {
            stream = new WrappedFSDataOutputStream(
                    this.wrappedFS.createNonRecursive(unwrappedPath, permission,
                            flags, bufferSize, replication, blockSize,
                            progress),
                    f, LiveOperationStatisticsAggregator.instance);
        } else {
            stream = this.wrappedFS.createNonRecursive(unwrappedPath,
                    permission, flags, bufferSize, replication, blockSize,
                    progress);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public Path createSnapshot(Path path, String snapshotName)
            throws IOException {
        UnwrappedPath unwrappedPath = unwrapPath(path);
        Path result = this.wrappedFS.createSnapshot(unwrappedPath,
                snapshotName);
        return unwrappedPath.isUnwrapped() ? wrapPath(result) : result;
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws AccessControlException, FileAlreadyExistsException,
            FileNotFoundException, ParentNotDirectoryException,
            UnsupportedFileSystemException, IOException {
        this.wrappedFS.createSymlink(unwrapPath(target), unwrapPath(link),
                createParent);
    }

    @Override
    @Deprecated
    public boolean delete(Path f) throws IOException {
        return this.wrappedFS.delete(unwrapPath(f));
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        boolean result = this.wrappedFS.delete(unwrappedPath, recursive);
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return result;
    }

    @Override
    public boolean deleteOnExit(Path f) throws IOException {
        return this.wrappedFS.deleteOnExit(unwrapPath(f));
    }

    @Override
    public void deleteSnapshot(Path path, String snapshotName)
            throws IOException {
        this.wrappedFS.deleteSnapshot(unwrapPath(path), snapshotName);
    }

    @Override
    public boolean exists(Path path) throws IOException {
        return this.wrappedFS.exists(unwrapPath(path));
    }

    @Override
    @Deprecated
    public long getBlockSize(Path f) throws IOException {
        return this.wrappedFS.getBlockSize(unwrapPath(f));
    }

    @Override
    public String getCanonicalServiceName() {
        return this.wrappedFS.getCanonicalServiceName();
    }

    @Override
    public FileSystem[] getChildFileSystems() {
        return this.wrappedFS.getChildFileSystems();
    }

    @Override
    public ContentSummary getContentSummary(Path path) throws IOException {
        return this.wrappedFS.getContentSummary(unwrapPath(path));
    }

    @Override
    @Deprecated
    public long getDefaultBlockSize() {
        return this.wrappedFS.getDefaultBlockSize();
    }

    @Override
    public long getDefaultBlockSize(Path f) {
        return this.wrappedFS.getDefaultBlockSize(unwrapPath(f));
    }

    @Override
    @Deprecated
    public short getDefaultReplication() {
        return this.wrappedFS.getDefaultReplication();
    }

    @Override
    public short getDefaultReplication(Path path) {
        return this.wrappedFS.getDefaultReplication(unwrapPath(path));
    }

    @Override
    public Token<?> getDelegationToken(String renewer) throws IOException {
        return this.wrappedFS.getDelegationToken(renewer);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
            long len) throws IOException {
        long startTime = System.currentTimeMillis();
        Path path = file.getPath();
        file.setPath(unwrapPath(path));
        BlockLocation[] blockLocations = this.wrappedFS
                .getFileBlockLocations(file, start, len);
        file.setPath(path);
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(file.getPath().toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return blockLocations;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
            throws IOException {
        long startTime = System.currentTimeMillis();
        Path path = unwrapPath(p);
        BlockLocation[] blockLocations = this.wrappedFS
                .getFileBlockLocations(path, start, len);
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(p.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return blockLocations;
    }

    @Override
    public FileChecksum getFileChecksum(Path f) throws IOException {
        return this.wrappedFS.getFileChecksum(unwrapPath(f));
    }

    @Override
    public FileStatus getFileLinkStatus(Path f) throws AccessControlException,
            FileNotFoundException, UnsupportedFileSystemException, IOException {
        UnwrappedPath unwrappedPath = unwrapPath(f);
        FileStatus fileStatus = this.wrappedFS.getFileLinkStatus(unwrappedPath);
        if (unwrappedPath.isUnwrapped()) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                    f.toUri().getAuthority()));
        }
        return fileStatus;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        UnwrappedPath unwrappedPath = unwrapPath(f);
        FileStatus fileStatus = this.wrappedFS.getFileStatus(unwrappedPath);
        if (unwrappedPath.isUnwrapped()) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                    f.toUri().getAuthority()));
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return fileStatus;
    }

    @Override
    public Path getHomeDirectory() {
        return wrapPath(this.wrappedFS.getHomeDirectory());
    }

    @Override
    @Deprecated
    public long getLength(Path f) throws IOException {
        return this.wrappedFS.getLength(unwrapPath(f));
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        UnwrappedPath unwrappedPath = unwrapPath(f);
        return unwrappedPath.isUnwrapped()
                ? wrapPath(this.wrappedFS.getLinkTarget(unwrapPath(f)))
                : this.wrappedFS.getLinkTarget(unwrapPath(f));
    }

    @Override
    @Deprecated
    public String getName() {
        return this.wrappedFS.getName();
    }

    @Override
    @Deprecated
    public short getReplication(Path src) throws IOException {
        return this.wrappedFS.getReplication(unwrapPath(src));
    }

    @Override
    public String getScheme() {
        return "sfs";
    }

    @Override
    @Deprecated
    public FsServerDefaults getServerDefaults() throws IOException {
        return this.wrappedFS.getServerDefaults();
    }

    @Override
    public FsServerDefaults getServerDefaults(Path p) throws IOException {
        return this.wrappedFS.getServerDefaults(unwrapPath(p));
    }

    @Override
    public FsStatus getStatus() throws IOException {
        return this.wrappedFS.getStatus();
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        return this.wrappedFS.getStatus(unwrapPath(p));
    }

    @Override
    public URI getUri() {
        return this.fileSystemUri;
    }

    @Override
    public long getUsed() throws IOException {
        return this.wrappedFS.getUsed();
    }

    @Override
    public Path getWorkingDirectory() {
        Path f = this.wrappedFS.getWorkingDirectory();
        Path wrappedWorkingDirectory = setAuthority(wrapPath(f),
                this.fileSystemUri.getAuthority());
        return wrappedWorkingDirectory;
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        UnwrappedPath unwrappedPathPattern = unwrapPath(pathPattern);
        FileStatus[] fileStatuses = this.wrappedFS
                .globStatus(unwrappedPathPattern);
        if (fileStatuses == null) {
            return null;
        }
        if (unwrappedPathPattern.isUnwrapped()) {
            for (FileStatus fileStatus : fileStatuses) {
                fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                        pathPattern.toUri().getAuthority()));
            }
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

        UnwrappedPath unwrappedPathPattern = unwrapPath(pathPattern);
        FileStatus[] fileStatuses = this.wrappedFS
                .globStatus(unwrappedPathPattern, wrappedFilter);
        if (fileStatuses == null) {
            return null;
        }
        if (unwrappedPathPattern.isUnwrapped()) {
            for (FileStatus fileStatus : fileStatuses) {
                fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                        pathPattern.toUri().getAuthority()));
            }
        }
        return fileStatuses;
    }

    @Override
    public boolean isDirectory(Path path) throws IOException {
        return this.wrappedFS.isDirectory(unwrapPath(path));
    }

    @Override
    public boolean isFile(Path path) throws IOException {
        return this.wrappedFS.isFile(unwrapPath(path));
    }

    @Override
    public RemoteIterator<Path> listCorruptFileBlocks(Path path)
            throws IOException {
        final UnwrappedPath unwrappedPath = unwrapPath(path);
        final RemoteIterator<Path> it = this.wrappedFS
                .listCorruptFileBlocks(unwrappedPath);
        return new RemoteIterator<Path>() {
            @Override
            public boolean hasNext() throws IOException {
                return it.hasNext();
            }

            @Override
            public Path next() throws IOException {
                return unwrappedPath.isUnwrapped() ? wrapPath(it.next())
                        : it.next();
            }
        };
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path f,
            boolean recursive) throws FileNotFoundException, IOException {
        final UnwrappedPath unwrappedPath = unwrapPath(f);
        final RemoteIterator<LocatedFileStatus> it = this.wrappedFS
                .listFiles(unwrappedPath, recursive);
        return new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return it.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                LocatedFileStatus fileStatus = it.next();
                if (unwrappedPath.isUnwrapped()) {
                    fileStatus.setPath(
                            setAuthority(wrapPath(fileStatus.getPath()),
                                    f.toUri().getAuthority()));
                }
                return fileStatus;
            }
        };
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)
            throws FileNotFoundException, IOException {
        final UnwrappedPath unwrappedPath = unwrapPath(f);
        final RemoteIterator<LocatedFileStatus> it = this.wrappedFS
                .listLocatedStatus(unwrappedPath);
        return new RemoteIterator<LocatedFileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return it.hasNext();
            }

            @Override
            public LocatedFileStatus next() throws IOException {
                LocatedFileStatus fileStatus = it.next();
                if (unwrappedPath.isUnwrapped()) {
                    fileStatus.setPath(
                            setAuthority(wrapPath(fileStatus.getPath()),
                                    f.toUri().getAuthority()));
                }
                return fileStatus;
            }
        };
    }

    @Override
    public FileStatus[] listStatus(Path f)
            throws FileNotFoundException, IOException {
        long startTime = System.currentTimeMillis();
        UnwrappedPath unwrappedPath = unwrapPath(f);
        FileStatus[] fileStatuses = this.wrappedFS.listStatus(unwrappedPath);
        if (unwrappedPath.isUnwrapped()) {
            for (FileStatus fileStatus : fileStatuses) {
                fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                        f.toUri().getAuthority()));
            }
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] listStatus(Path f, PathFilter filter)
            throws FileNotFoundException, IOException {
        long startTime = System.currentTimeMillis();
        UnwrappedPath unwrappedPath = unwrapPath(f);
        PathFilter wrappedFilter = new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return filter.accept(unwrapPath(path));
            }
        };

        FileStatus[] fileStatuses = this.wrappedFS.listStatus(unwrappedPath,
                wrappedFilter);
        if (unwrappedPath.isUnwrapped()) {
            for (FileStatus fileStatus : fileStatuses) {
                fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()),
                        f.toUri().getAuthority()));
            }
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] listStatus(Path[] files)
            throws FileNotFoundException, IOException {
        UnwrappedPath[] unwrappedFiles = new UnwrappedPath[files.length];
        for (int i = 0; i < files.length; ++i) {
            unwrappedFiles[i] = unwrapPath(files[i]);
        }

        FileStatus[] fileStatuses = this.wrappedFS.listStatus(unwrappedFiles);
        for (int i = 0; i < fileStatuses.length; ++i) {
            if (unwrappedFiles[i].isUnwrapped()) {
                fileStatuses[i].setPath(
                        setAuthority(wrapPath(fileStatuses[i].getPath()),
                                files[i].toUri().getAuthority()));
            }
        }
        return fileStatuses;
    }

    @Override
    public FileStatus[] listStatus(Path[] path, PathFilter filter)
            throws FileNotFoundException, IOException {
        UnwrappedPath[] unwrappedPaths = new UnwrappedPath[path.length];
        for (int i = 0; i < path.length; ++i) {
            unwrappedPaths[i] = unwrapPath(path[i]);
        }

        PathFilter wrappedFilter = new PathFilter() {
            @Override
            public boolean accept(Path p) {
                return filter.accept(unwrapPath(p));
            }
        };

        FileStatus[] fileStatuses = this.wrappedFS.listStatus(unwrappedPaths,
                wrappedFilter);
        for (int i = 0; i < fileStatuses.length; ++i) {
            if (unwrappedPaths[i].isUnwrapped()) {
                fileStatuses[i].setPath(
                        setAuthority(wrapPath(fileStatuses[i].getPath()),
                                path[i].toUri().getAuthority()));
            }
        }
        return fileStatuses;
    }

    @Override
    public Path makeQualified(Path path) {
        UnwrappedPath unwrappedPath = unwrapPath(path);
        return unwrappedPath.isUnwrapped()
                ? wrapPath(this.wrappedFS.makeQualified(unwrappedPath))
                : this.wrappedFS.makeQualified(unwrappedPath);
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return this.wrappedFS.mkdirs(unwrapPath(f));
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        boolean result = this.wrappedFS.mkdirs(unwrappedPath, permission);
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return result;
    }

    @Override
    public void moveFromLocalFile(Path src, Path dst) throws IOException {
        this.wrappedFS.moveFromLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException {
        Path[] unwrappedSrcs = new Path[srcs.length];
        for (int i = 0; i < srcs.length; ++i) {
            unwrappedSrcs[i] = unwrapPath(srcs[i]);
        }
        this.wrappedFS.moveFromLocalFile(unwrappedSrcs, unwrapPath(dst));
    }

    @Override
    public void moveToLocalFile(Path src, Path dst) throws IOException {
        this.wrappedFS.moveToLocalFile(unwrapPath(src), unwrapPath(dst));
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataInputStream stream;
        if (!this.skipRead) {
            stream = new FSDataInputStream(new WrappedFSDataInputStream(
                    this.wrappedFS.open(unwrappedPath), f,
                    LiveOperationStatisticsAggregator.instance,
                    this.skipOther));
        } else {
            stream = this.wrappedFS.open(unwrappedPath);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataInputStream stream;
        if (!this.skipRead) {
            stream = new FSDataInputStream(new WrappedFSDataInputStream(
                    this.wrappedFS.open(unwrappedPath, bufferSize), f,
                    LiveOperationStatisticsAggregator.instance,
                    this.skipOther));
        } else {
            stream = this.wrappedFS.open(unwrappedPath, bufferSize);
        }
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(f.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return stream;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedSrc = unwrapPath(src);
        Path unwrappedDst = unwrapPath(dst);
        boolean result = this.wrappedFS.rename(unwrappedSrc, unwrappedDst);
        if (!this.skipOther) {
            int fd = LiveOperationStatisticsAggregator.instance
                    .registerFileDescriptor(src.toString());
            LiveOperationStatisticsAggregator.instance
                    .aggregateOperationStatistics(OperationSource.SFS,
                            OperationCategory.OTHER, startTime,
                            System.currentTimeMillis(), fd);
        }
        return result;
    }

    @Override
    public void renameSnapshot(Path path, String snapshotOldName,
            String snapshotNewName) throws IOException {
        this.wrappedFS.renameSnapshot(unwrapPath(path), snapshotOldName,
                snapshotNewName);
    }

    @Override
    public Path resolvePath(Path p) throws IOException {
        UnwrappedPath unwrappedPath = unwrapPath(p);
        return unwrappedPath.isUnwrapped()
                ? wrapPath(this.wrappedFS.resolvePath(unwrappedPath))
                : this.wrappedFS.resolvePath(unwrappedPath);
    }

    @Override
    public void setOwner(Path p, String username, String groupname)
            throws IOException {
        this.wrappedFS.setOwner(unwrapPath(p), username, groupname);
    }

    @Override
    public void setPermission(Path p, FsPermission permission)
            throws IOException {
        this.wrappedFS.setPermission(unwrapPath(p), permission);
    }

    @Override
    public boolean setReplication(Path src, short replication)
            throws IOException {
        return this.wrappedFS.setReplication(unwrapPath(src), replication);
    }

    @Override
    public void setTimes(Path p, long mtime, long atime) throws IOException {
        this.wrappedFS.setTimes(unwrapPath(p), mtime, atime);
    }

    @Override
    public void setVerifyChecksum(boolean verifyChecksum) {
        this.wrappedFS.setVerifyChecksum(verifyChecksum);
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        Path unwrappedPath = unwrapPath(new_dir);
        this.wrappedFS.setWorkingDirectory(unwrappedPath);
    }

    @Override
    public void setWriteChecksum(boolean writeChecksum) {
        this.wrappedFS.setWriteChecksum(writeChecksum);
    }

    @Override
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
            throws IOException {
        UnwrappedPath unwrappedFsOutputFile = unwrapPath(fsOutputFile);
        return unwrappedFsOutputFile.isUnwrapped()
                ? wrapPath(this.wrappedFS
                        .startLocalOutput(unwrappedFsOutputFile, tmpLocalFile))
                : this.wrappedFS.startLocalOutput(unwrappedFsOutputFile,
                        tmpLocalFile);
    }

    @Override
    public boolean supportsSymlinks() {
        return this.wrappedFS.supportsSymlinks();
    }

    // Helper methods.

    private static URI replaceUriScheme(URI uri, String from, String to) {
        // TODO add cache for replaced URIs? possibly useful for scenarios with
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
                return null;
            } else {
                // uri has wrong scheme
                return null;
            }
        }

        // uri has no scheme
        return null;
    }

    static Path setAuthority(Path path, String authority) {
        if (authority != null) {
            URI pathUri = path.toUri();
            String query = pathUri.getQuery();
            String fragment = pathUri.getFragment();
            return new Path(URI
                    .create(pathUri.getScheme() + "://" + authority + "/"
                            + pathUri.getPath()
                            + (query != null ? ("?" + query) : ""))
                    + (fragment != null ? ("#" + fragment) : ""));
        }

        return path;
    }

    UnwrappedPath unwrapPath(Path path) {
        URI unwrappedUri = replaceUriScheme(path.toUri(), getScheme(),
                this.wrappedFSScheme);

        // if the returned URI is null, then the path has not been unwrapped,
        // either because it has no scheme, it has the wrong scheme, or it
        // already has the correct scheme
        return unwrappedUri != null ? new UnwrappedPath(unwrappedUri, true)
                : new UnwrappedPath(path.toUri(), false);
    }

    Path wrapPath(Path path) {
        // only wrap the path if it has been unwrapped before
        return new Path(replaceUriScheme(path.toUri(), this.wrappedFSScheme,
                getScheme()));
    }

    private static class UnwrappedPath extends Path {
        private final boolean unwrapped;

        public UnwrappedPath(URI aUri) {
            super(aUri);
            this.unwrapped = false;
        }

        public UnwrappedPath(URI aUri, boolean unwrapped) {
            super(aUri);
            this.unwrapped = unwrapped;
        }

        public boolean isUnwrapped() {
            return this.unwrapped;
        }
    }
}
