package org.apache.hadoop.examples;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.apress.hadoopbook.ClusterMapReduceDelegate;

public class PiEstimatorTest extends ClusterMapReduceDelegate {
	public static Logger LOG = Logger.getLogger(PiEstimatorTest.class);
	
	/** This is the JUnit4 before class, cluster initialization method.
	 * 
	 * This method starts the cluster and performs simple validation of the working state of the cluster.
	 * 
	 * Under some failure cases usually related to incorrect CLASSPATH configuration, this method may never complete.
	 * 
	 * If all of the test cases in your file can share a cluster, use the @BeforeClass annotation.
	 * @throws Exception
	 */
	@BeforeClass
	public static void startVirtualCluster() throws Exception
	{
		/** Turn down the cluster logging to filter the noise out. Do this if the test is basically working. */
		final String rootLogLevel = System.getProperty("virtual.cluster.logLevel","WARN");
		final String testLogLevel = System.getProperty("test.log.level", "INFO");
		LOG.info("Setting Log Level to " + rootLogLevel);
		LogManager.getRootLogger().setLevel(Level.toLevel(rootLogLevel));
		
		/** Turn up the logging on this class and the delegate. */
		LOG.setLevel(Level.toLevel(testLogLevel));
		ClusterMapReduceDelegate.LOG.setLevel(Level.toLevel(testLogLevel));
		
		
		setupTestClass(false);
		/** Verify that there is a JobConf object for the cluster. */
		assertNotNull("Cluster initialized Correctly", getConf());
		/** Verify that the file system object is available. */
		assertNotNull("Cluster has a file system", getFs());
	}
	
	/** This is the JUnit4 after class tear down method.
	 * This stops the cluster, and would perform any needed cleanup.
	 * If all of the test cases in a file can share a cluster use the @AfterClass annotation.
	 * @throws Exception
	 */
	@AfterClass
	public static void stopVirtualCluster() throws Exception {
		/** Put a break point on this method if you are debugging and want to inspect the cluster
		 * via looking directly at hdfs or via the web interfaces.
		 */
		teardownTestClass();
	}
	

	@Test
	public void testLaunch() throws Exception {
		final FileSystem fs = getFs();
		final JobConf testBaseConf = getConf();
		LOG.info( "The HDFS url is " + fs.getUri());
		LOG.info( "The Jobtracker URL is " + getJobtrackerURL());
		LOG.info( "The Namenode URL is " + getNamenodeURL());
		
		/** Make a new {@link JobConf} object that is setup to ensure that the jar containing {@link PiEstimator}
		 * is available to the TaskTrackers.
		 * 
		 * Note: It is very bad practice to modify the configuration given back by getConf() as the returned object is shared among
		 * all tests in the Test file.
		 */
		JobConf conf = new JobConf(testBaseConf);
		/** Make all task output come to the console of the unit test. */
		conf.set("jobclient.output.filter","ALL");
		
		/** Insure that hadoop- -examples.jar is pushed into the DistributedCache and made available to the TaskTrackers.
		 * 
		 */
		conf.setJarByClass(PiEstimator.class);
		
		/** Create the PiEstimator object and initialize it with our conf object. */
		PiEstimator toTest = new PiEstimator();
		toTest.setConf(conf);
		
		int maps = 10;
		long samples = 1000;
		double result = toTest.launch(maps, samples, null, null);
		LOG.info( "The computed result for pi is " + result);
		assertTrue( "Result Pi >3 ", result > 3);
		assertTrue( "Result Pi <4 ", result < 4);
		
	}

}
