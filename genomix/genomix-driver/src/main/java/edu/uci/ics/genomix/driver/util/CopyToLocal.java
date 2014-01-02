package edu.uci.ics.genomix.driver.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.data.cluster.GenomixClusterManager;

@SuppressWarnings("deprecation")
public class CopyToLocal {
    public static void main(String[] args) throws CmdLineException, IOException {
        Configuration conf = new Configuration();
        String PATH_TO_HADOOP_CONF = System.getenv("HADOOP_HOME") + "/conf";
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));
        JobConf job = new JobConf(conf);
        if (args.length < 2) {
            System.err.println("Usage: copytolocal <hdfsSrcDir> <localDestDir>");
            return;
        }
        String hdfsSrcDir = args[0];
        String localDestDir = args[1];
        GenomixClusterManager.copyBinAndTextToLocal(job, hdfsSrcDir, localDestDir);
    }
}
