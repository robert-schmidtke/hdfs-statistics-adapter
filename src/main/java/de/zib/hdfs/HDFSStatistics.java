/*
 * Copyright (c) 016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileSystem;

public class HDFSStatistics extends DelegateToFileSystem {

    protected HDFSStatistics(URI theUri, FileSystem theFsImpl,
            Configuration conf, String supportedScheme,
            boolean authorityRequired) throws IOException, URISyntaxException {
        super(theUri, theFsImpl, conf, supportedScheme, authorityRequired);
    }
    
    protected HDFSStatistics(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        this(theUri, new HDFSStatisticsFileSystem(), conf, theUri.getScheme(), false);
    }

}
