package com.apress.hadoopbook;

import java.io.FileOutputStream;

import org.apache.hadoop.mapred.JobConf;

public class RunVirtualCluster extends ClusterMapReduceDelegate {
	

	static public void main(String[] args) throws Exception {
		try {
			setupTestClass(false);	/** Start a virtual cluster */
			JobConf conf = getConf();
			if (conf==null) {
				System.err.println( "The virtual cluster failed to start");
				return;
			}
			System.out.println( "The hdfs url is " + getHdfsURI());
			System.out.println( "The jobtracker address is " + conf.get("mapred.job.tracker") );
			System.out.println( "The namenode web ui is " + getNamenodeURL());
			System.out.println( "The Jobtracker web ui is " + getJobtrackerURL());
			if (args.length>0) {
				JobConf dummy = new JobConf(false);
				dummy.set("fs.default.name", getHdfsURI().toString());
				dummy.set("mapred.job.tracker", conf.get("mapred.job.tracker"));
				dummy.set("dfs.http.address", getNamenodeURL());
				dummy.set("mapred.job.tracker.http.address", getJobtrackerURL());
				FileOutputStream out = new FileOutputStream( args[0]);
				dummy.writeXml(out);
				out.close();
				System.out.println( "Configuration written to " + args[0]);
				
				
			}
			/** Wait until killed. */
			synchronized(conf) {
				conf.wait();
			}
		} finally {
			teardownTestClass();
		}
		
	}



}
