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
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Random;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

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

import de.zib.sfs.agent.StatisticsFileSystemAgent;
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
     * Location of the log file on each host. A random suffix will be appended.
     */
    public static final String SFS_LOG_FILE_NAME_KEY = "sfs.logFile.name";

    /**
     * Flag to indicate whether to delete the local log file during {
     * {@link #close()}
     */
    public static final String SFS_DELETE_LOG_FILE_ON_CLOSE_KEY = "sfs.logFile.deleteOnClose";

    /**
     * Directory to copy the host log file to during {@link #close()}.
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
     * Whether to delete the local log file during {@link #close()}. Default:
     * {@code false}.
     */
    private boolean deleteLogFileOnClose;

    /**
     * Path to copy the generated log file to during {@link #close()}. Default:
     * {@code null}.
     */
    private String targetLogFileDirectory;

    /**
     * Agent that monitors low level file system interaction.
     */
    private StatisticsFileSystemAgent agent;

    /**
     * Flag to track whether this file system is closed already.
     */
    private boolean closed = false;

    /**
     * Flag to track whether this file system is initialized already.
     */
    private boolean initialized = false;

    // Shadow super class' LOG
    public static final Log LOG = LogFactory.getLog(StatisticsFileSystem.class);

    /**
     * For generating random suffixes to log file names.
     */
    private static final Random RANDOM = new Random();

    @Override
    public synchronized void initialize(URI name, Configuration conf)
            throws IOException {
        if (initialized) {
            LOG.warn("Ignoring attempt to re-initialize file system.");
            return;
        }

        super.initialize(name, conf);
        setConf(conf);

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

        // Append 5-character random string to avoid collisions if multiple
        // instances are running on the same machine
        logFileName += "." + new BigInteger(25, RANDOM).toString(32);
        logFile = new File(logFileName);

        if (!logFile.getParentFile().exists()) {
            if (!logFile.getParentFile().mkdirs()) {
                throw new RuntimeException(
                        "Could not create log file directories: "
                                + logFile.getParentFile().getAbsolutePath());
            }
        }

        System.setProperty("de.zib.sfs.asyncLogFileName", logFileName);
        System.setProperty("de.zib.sfs.hostname", hostname);

        fsLogger = LogManager.getLogger("de.zib.sfs.AsyncLogger");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Initialized file system logger");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Logging to " + logFileName);
        }

        deleteLogFileOnClose = getConf().getBoolean(
                SFS_DELETE_LOG_FILE_ON_CLOSE_KEY, false);

        // Get the target log file directory
        targetLogFileDirectory = getConf().get(
                SFS_TARGET_LOG_FILE_DIRECTORY_KEY);

        // Inject the agent that monitors low-level file system access
        try {
            StringBuilder agentOptions = new StringBuilder();
            agentOptions
                    .append(StatisticsFileSystemAgent.SFS_AGENT_LOGGER_NAME_KEY)
                    .append("=").append("de.zib.sfs.AsyncLogger");
            agent = StatisticsFileSystemAgent
                    .loadAgent(agentOptions.toString());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Injected low-level file system logger agent");
            }
        } catch (Exception e) {
            LOG.warn("Could not inject low-level file system logger agent", e);
        }

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

        // Add shutdown hook that closes this file system
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    StatisticsFileSystem.this.close();
                } catch (IOException e) {
                    LOG.error("Could not close file system", e);
                }
            }
        });

        initialized = true;
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream = new WrappedFSDataOutputStream(
                wrappedFS.append(unwrappedPath, bufferSize, progress), fsLogger);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.append({},{}):{}", duration, this, f, bufferSize,
                stream);
        return stream;
    }

    @Override
    public synchronized final void close() throws IOException {
        if (closed) {
            return;
        }

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

            // The appender rolls over at a certain size (see
            // src/main/resources/log4j2.xml) and creates a .gz archive named
            // like the original log file, plus an additional counter, e.g.
            // file.log.1.gz, where file.log is the original log file name. So
            // enumerate all these files and copy them.
            java.nio.file.Path fromPath = Paths.get(logFile.getAbsolutePath());

            // accepts any file whose name starts exactly with the log file's
            // name, so any suffixes to these files are accepts as well
            BiPredicate<java.nio.file.Path, BasicFileAttributes> logFilePredicate = new BiPredicate<java.nio.file.Path, BasicFileAttributes>() {
                @Override
                public boolean test(java.nio.file.Path path,
                        BasicFileAttributes attributes) {
                    boolean accept = path.toFile().getName()
                            .startsWith(logFile.getName());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug((accept ? "Accepting" : "Not accepting")
                                + " file " + path
                                + " (its name must start with "
                                + logFile.getName() + ")");
                    }
                    return accept;
                }
            };

            // copies any file to the target log file directory
            Consumer<java.nio.file.Path> logFileConsumer = new Consumer<java.nio.file.Path>() {
                @Override
                public void accept(java.nio.file.Path path) {
                    java.nio.file.Path toPath = Paths.get(
                            targetLogFileDirectoryFile.getAbsolutePath(),
                            hostname + "-" + path.toFile().getName());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Copying log from " + fromPath + " to "
                                + toPath);
                    }
                    try {
                        Files.copy(path, toPath);
                    } catch (FileAlreadyExistsException e) {
                        LOG.warn("Log file " + toPath + " already exists", e);
                    } catch (IOException e) {
                        LOG.warn("Error copying log file to " + toPath, e);
                    }

                    if (deleteLogFileOnClose && !path.toFile().delete()) {
                        LOG.warn("Could not delete log file " + path);
                    }
                }
            };

            // finally walk all files (no recursion) and copy the matching log
            // files
            Files.find(fromPath.getParent(), 1, logFilePredicate).forEach(
                    logFileConsumer);
        }

        closed = true;
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FSDataOutputStream stream = new WrappedFSDataOutputStream(
                wrappedFS.create(unwrappedPath, permission, overwrite,
                        bufferSize, replication, blockSize, progress), fsLogger);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.create({},{},{},{},{},{}):{}", duration, this, f,
                permission, overwrite, bufferSize, replication, blockSize,
                stream);
        return stream;
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        boolean result = wrappedFS.delete(unwrappedPath, recursive);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.delete({},{}):{}", duration, this, f, recursive,
                result);
        return result;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
            long len) throws IOException {
        long startTime = System.currentTimeMillis();
        FileStatus unwrappedFile = new FileStatus(file.getLen(),
                file.isDirectory(), file.getReplication(), file.getBlockSize(),
                file.getModificationTime(), file.getAccessTime(),
                file.getPermission(), file.getOwner(), file.getGroup(),
                (file.isSymlink() ? file.getSymlink() : null),
                unwrapPath(file.getPath()));
        BlockLocation[] blockLocations = wrappedFS.getFileBlockLocations(
                unwrappedFile, start, len);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.getFileBlockLocations({},{},{}):{}", duration,
                this, file, start, len, Arrays.toString(blockLocations));
        return blockLocations;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FileStatus fileStatus = wrappedFS.getFileStatus(unwrappedPath);
        fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()), f
                .toUri().getAuthority()));
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.getFileStatus({}):{}", duration, this, f,
                fileStatus);
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
        Path wrappedWorkingDirectory = setAuthority(wrapPath(f),
                fileSystemUri.getAuthority());
        return wrappedWorkingDirectory;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException,
            IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        FileStatus[] fileStatuses = wrappedFS.listStatus(unwrappedPath);
        for (FileStatus fileStatus : fileStatuses) {
            fileStatus.setPath(setAuthority(wrapPath(fileStatus.getPath()), f
                    .toUri().getAuthority()));
        }
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.listStatus({}):{}", duration, this, f,
                Arrays.toString(fileStatuses));
        return fileStatuses;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        boolean result = wrappedFS.mkdirs(unwrappedPath, permission);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.mkdirs({},{}):{}", duration, this, f, permission,
                result);
        return result;
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedPath = unwrapPath(f);
        WrappedFSDataInputStream stream = new WrappedFSDataInputStream(
                wrappedFS.open(unwrappedPath, bufferSize), fsLogger);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.open({},{}):{}", duration, this, f, bufferSize,
                stream);
        return new FSDataInputStream(stream);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        long startTime = System.currentTimeMillis();
        Path unwrappedSrc = unwrapPath(src);
        Path unwrappedDst = unwrapPath(dst);
        boolean result = wrappedFS.rename(unwrappedSrc, unwrappedDst);
        long duration = System.currentTimeMillis() - startTime;
        fsLogger.info("{}:{}.rename({},{}):{}", duration, this, src, dst,
                result);
        return result;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        Path unwrappedPath = unwrapPath(new_dir);
        wrappedFS.setWorkingDirectory(unwrappedPath);
    }

    // Helper methods.

    private Path setAuthority(Path path, String authority) {
        if (authority != null) {
            URI pathUri = path.toUri();
            String query = pathUri.getQuery();
            String fragment = pathUri.getFragment();
            return new Path(URI.create(pathUri.getScheme() + "://" + authority
                    + "/" + pathUri.getPath()
                    + (query != null ? ("?" + query) : ""))
                    + (fragment != null ? ("#" + fragment) : ""));
        } else {
            return path;
        }
    }

    private Path wrapPath(Path path) {
        return new Path(replaceUriScheme(path.toUri(), wrappedFSScheme,
                getScheme()));
    }

    private Path unwrapPath(Path path) {
        return new Path(replaceUriScheme(path.toUri(), getScheme(),
                wrappedFSScheme));
    }

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
