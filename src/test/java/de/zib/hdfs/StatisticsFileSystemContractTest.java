/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.hdfs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class StatisticsFileSystemContractTest extends
        FileSystemContractBaseTest {

    private final Path fileSystemPath;

    private final Configuration conf;

    private MiniDFSCluster cluster;

    private static final Log LOG = LogFactory
            .getLog(FileSystemContractBaseTest.class);

    public StatisticsFileSystemContractTest() {
        this("sfs://localhost:8020");
    }

    protected StatisticsFileSystemContractTest(String fileSystemUri) {
        fileSystemPath = new Path(fileSystemUri);

        Configuration.addDefaultResource("hadoop/core-site.xml");
        conf = new HdfsConfiguration();
        conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,
                FileSystemContractBaseTest.TEST_UMASK);

        // most test use tiny block sizes, so disable minimum block size
        conf.set(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, "0");

        // set NameNode and DataNode directories
        System.setProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA,
                "target/test/data");
    }

    @Override
    protected void setUp() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing test '" + getName() + "'");
        }
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1)
                .nameNodePort(8020).build();
        fs = FileSystem.get(fileSystemPath.toUri(), conf);
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (cluster != null) {
            cluster.shutdown();
            cluster = null;
        }
    }
}
