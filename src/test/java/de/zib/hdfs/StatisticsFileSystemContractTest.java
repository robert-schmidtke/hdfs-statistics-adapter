/*
 * Copyright (c) 2016 by Robert Schmidtke,
 *               Zuse Institute Berlin
 *
 * Licensed under the BSD License, see LICENSE file for details.
 *
 */
package de.zib.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

public class StatisticsFileSystemContractTest extends
        FileSystemContractBaseTest {

    private final Path fileSystemPath;

    private final Configuration conf;

    public StatisticsFileSystemContractTest() {
        this("sfs://localhost:8020");
    }

    public StatisticsFileSystemContractTest(String fileSystemUri) {
        fileSystemPath = new Path(fileSystemUri);

        Configuration
                .addDefaultResource(StatisticsFileSystemContractTest.class
                        .getClassLoader().getResource("hadoop/core-site.xml")
                        .getPath());
        conf = new Configuration();
    }

    @Override
    protected void setUp() throws Exception {
        fs = FileSystem.get(fileSystemPath.toUri(), conf);
        super.setUp();
    }
}
