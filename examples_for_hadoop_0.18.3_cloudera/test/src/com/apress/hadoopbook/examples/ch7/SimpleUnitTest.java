/**
 * 
 */
package com.apress.hadoopbook.examples.ch7;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.apress.hadoopbook.ClusterMapReduceDelegate;
import com.apress.hadoopbook.utils.Utils;

/** This simple unit test exists to demonstrate the creation and tear down of a virtual cluster and
 * the writing of a test case that uses the created cluster.
 * 
 * @author Jason
 *
 */
public class SimpleUnitTest extends ClusterMapReduceDelegate {
	public static Logger LOG = Logger.getLogger(SimpleUnitTest.class);
	
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
//		final String rootLogLevel = System.getProperty("virtual.cluster.logLevel","WARN");
//		final String testLogLevel = System.getProperty("test.log.level", "INFO");
//		LOG.info("Setting Log Level to " + rootLogLevel);
//		LogManager.getRootLogger().setLevel(Level.toLevel(rootLogLevel));
//		LOG.setLevel(Level.toLevel(testLogLevel));
//		
		
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
		teardownTestClass();
	}
	
	/** A very simple unit test that uses the virtual cluster.
	 * 
	 * The test case writes a single file to HDFS and reads it back, verifying the file contents.
	 * 
	 * @throws Exception
	 */
	@Test
	public void createFileInHdfs() throws Exception
	{
		final FileSystem fs = getFs();
		assertEquals( "File System is hdfs", "hdfs", fs.getUri().getScheme());
		
		Path testFile = new Path("testFile");
		FSDataOutputStream out = null;
		FSDataInputStream in = null;
		final String testData = "HelloWorld";
		try {
			/** Create our test file and write our test string to it. The writeUTF method writes some header information to the file. */
			out = fs.create(testFile,false);
			out.writeUTF(testData);
			/** With HDFS the file really doesn't exist until after it has been closed. */
			out.close();
			out = null;

			/** verify that the file exists. open it and read the data back and verify the data. */
			assertTrue( "Test File " + testFile + " exists", fs.exists(testFile));
			in = fs.open(testFile);
			String readBack = in.readUTF();
			assertEquals("Read our test data back: " + testData, testData, readBack);
			in.close();
			in=null;
			
		} finally {
			/** Our traditional finally when descriptors were opened to ensure they are closed. */
			Utils.closeIf(out);
			Utils.closeIf(in);
		}
	}

}
