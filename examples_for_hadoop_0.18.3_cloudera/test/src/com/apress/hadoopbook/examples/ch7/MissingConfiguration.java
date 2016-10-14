package com.apress.hadoopbook.examples.ch7;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.apress.hadoopbook.ClusterMapReduceDelegate;
/** Class to demonstrate the various failures due to different configuration issues.
 * We are going to fail at running several clusters here, so we will not follow the suggested practice of using
 * {@link org.junit.AfterClass} and {@link org.junit.BeforeClass}. 
 * 
 * @author Jason
 *
 */
public class MissingConfiguration extends ClusterMapReduceDelegate {
	public static Logger LOG = Logger.getLogger(MissingConfiguration.class);


	@Test
	public void  missingJetty() throws Exception {

		

		setupDefaultSystemPropertiesIf();
	// FIXME, why does this call cause the job to fail?
//		logDefaults(  LOG);
		try { /** Ensure the cluster is stopped. */
			teardownTestClass();
		} catch (Throwable ignore) {}
		
		try {
			JobConf conf;
			setupTestClass(false);
			conf = createJobConf();
			logConfiguration( conf, LOG);
		
			FileSystem fs = getFileSystem();
			Path dummyFile = new Path( getTestRootDir(), "dummy");
			FSDataOutputStream out = fs.create(dummyFile, true);
			out.writeUTF("TestCaseString");
			out.close();
			FileStatus status = fs.getFileStatus(dummyFile);
			assertNotNull("dummy file must exist", status);


		} finally {
			try {
				teardownTestClass();
			} catch (Throwable ignore) {}
			
		
		}
		

	}
	

}
