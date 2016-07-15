/*
 * Copyright (c) 016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HDFSStatisticsFileSystem extends FileSystem {

    private URI fileSystemUri;

    private FileSystem hdfs;

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
        setConf(conf);

        try {
            hdfs = get(new URI("hdfs://"), getConf());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        fileSystemUri = URI.create(getScheme() + "://" + name.getHost()
                + (name.getPort() != -1 ? ":" + name.getPort() : "")
                + name.getPath());
    }

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
            throws IOException {
        return hdfs.append(arg0, arg1);
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1,
            boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
            throws IOException {
        return hdfs.create(arg0, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
        return hdfs.delete(arg0, arg1);
    }

    @Override
    public FileStatus getFileStatus(Path arg0) throws IOException {
        return hdfs.getFileStatus(arg0);
    }

    @Override
    public URI getUri() {
        return fileSystemUri;
    }

    @Override
    public String getScheme() {
        return "hdfss";
    }

    @Override
    public Path getWorkingDirectory() {
        return hdfs.getWorkingDirectory();
    }

    @Override
    public FileStatus[] listStatus(Path arg0) throws FileNotFoundException,
            IOException {
        return hdfs.listStatus(arg0);
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
        return hdfs.mkdirs(arg0, arg1);
    }

    @Override
    public FSDataInputStream open(Path arg0, int arg1) throws IOException {
        return hdfs.open(arg0, arg1);
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
        return hdfs.rename(arg0, arg1);
    }

    @Override
    public void setWorkingDirectory(Path arg0) {
        hdfs.setWorkingDirectory(arg0);
    }
}
