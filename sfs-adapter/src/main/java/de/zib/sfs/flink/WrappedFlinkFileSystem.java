/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.sfs.flink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Maps {@link org.apache.hadoop.fs.FileSystem} calls to an underlying
 * {@link org.apache.flink.core.fs.FileSystem} implementation.
 * 
 * @author robert
 *
 */
public class WrappedFlinkFileSystem extends FileSystem {

    /**
     * The underlying Flink {@link org.apache.flink.core.fs.FileSystem}
     * implementation to wrap.
     */
    private final org.apache.flink.core.fs.FileSystem wrappedFlinkFS;

    public WrappedFlinkFileSystem(
            final org.apache.flink.core.fs.FileSystem wrappedFlinkFS) {
        this.wrappedFlinkFS = wrappedFlinkFS;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        wrappedFlinkFS.initialize(name);
    }

    @Override
    public void close() throws IOException {
        // avoid calling super.close();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize,
            Progressable progress) throws IOException {
        throw new UnsupportedOperationException(
                "append is not supported in " + getClass().getSimpleName());
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
            boolean overwrite, int bufferSize, short replication,
            long blockSize, Progressable progress) throws IOException {
        org.apache.flink.core.fs.FSDataOutputStream out = wrappedFlinkFS.create(
                toFlinkPath(f), overwrite, bufferSize, replication, blockSize);
        statistics.incrementWriteOps(1);
        return toHadoopFSDataOutputStream(out, statistics);
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        statistics.incrementWriteOps(1);
        return wrappedFlinkFS.delete(toFlinkPath(f), recursive);
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
            long len) throws IOException {
        org.apache.flink.core.fs.BlockLocation[] flinkLocations = wrappedFlinkFS
                .getFileBlockLocations(toFlinkFileStatus(file), start, len);
        BlockLocation[] hadoopLocations = new BlockLocation[flinkLocations.length];
        statistics.incrementReadOps(1);
        for (int i = 0; i < hadoopLocations.length; ++i) {
            hadoopLocations[i] = toHadoopBlockLocation(flinkLocations[i]);
        }
        return hadoopLocations;
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        org.apache.flink.core.fs.FileStatus status = wrappedFlinkFS
                .getFileStatus(toFlinkPath(f));
        statistics.incrementReadOps(1);
        return toHadoopFileStatus(status);
    }

    @Override
    public URI getUri() {
        return wrappedFlinkFS.getUri();
    }

    @Override
    public String getScheme() {
        return getUri().getScheme();
    }

    @Override
    public Path getWorkingDirectory() {
        return toHadoopPath(wrappedFlinkFS.getWorkingDirectory());
    }

    @Override
    public FileStatus[] listStatus(Path f)
            throws FileNotFoundException, IOException {
        org.apache.flink.core.fs.FileStatus[] flinkStatuses = wrappedFlinkFS
                .listStatus(toFlinkPath(f));
        FileStatus[] hadoopStatuses = new FileStatus[flinkStatuses.length];
        statistics.incrementLargeReadOps(1);
        for (int i = 0; i < hadoopStatuses.length; ++i) {
            hadoopStatuses[i] = toHadoopFileStatus(flinkStatuses[i]);
        }
        return hadoopStatuses;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        statistics.incrementWriteOps(1);
        return wrappedFlinkFS.mkdirs(toFlinkPath(f));
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        org.apache.flink.core.fs.FSDataInputStream in = wrappedFlinkFS
                .open(toFlinkPath(f), bufferSize);
        statistics.incrementReadOps(1);
        return toHadoopFSDataInputStream(in, statistics);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        statistics.incrementWriteOps(1);
        return wrappedFlinkFS.rename(toFlinkPath(src), toFlinkPath(dst));
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        throw new UnsupportedOperationException(
                "setWorkingDirectory is not supported in "
                        + getClass().getSimpleName());
    }

    // Helper methods

    private static org.apache.flink.core.fs.FileStatus toFlinkFileStatus(
            final FileStatus status) {
        return new org.apache.flink.runtime.fs.hdfs.HadoopFileStatus(status);
    }

    private static org.apache.flink.core.fs.Path toFlinkPath(Path f) {
        return new org.apache.flink.core.fs.Path(f.toUri());
    }

    private static Path toHadoopPath(org.apache.flink.core.fs.Path f) {
        return new Path(f.toUri());
    }

    private static BlockLocation toHadoopBlockLocation(
            org.apache.flink.core.fs.BlockLocation location)
            throws IOException {
        return new BlockLocation(null, location.getHosts(),
                location.getOffset(), location.getLength());
    }

    private static FileStatus toHadoopFileStatus(
            org.apache.flink.core.fs.FileStatus status) {
        return new FileStatus(status.getLen(), status.isDir(),
                status.getReplication(), status.getBlockSize(),
                status.getModificationTime(), toHadoopPath(status.getPath()));
    }

    private static FSDataInputStream toHadoopFSDataInputStream(
            final org.apache.flink.core.fs.FSDataInputStream in,
            final FileSystem.Statistics statistics) throws IOException {
        return new FSDataInputStream(
                new WrappedFlinkFSDataInputStream(in, statistics));
    }

    private static FSDataOutputStream toHadoopFSDataOutputStream(
            org.apache.flink.core.fs.FSDataOutputStream out,
            final FileSystem.Statistics statistics) throws IOException {
        return new FSDataOutputStream(out, statistics);
    }

}
