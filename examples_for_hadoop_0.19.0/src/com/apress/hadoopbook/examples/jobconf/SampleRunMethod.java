package com.apress.hadoopbook.examples.jobconf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class SampleRunMethod {

	/** Code Fragment to demonstrate a run method that can be called from a unit test, or from a driver that launches multipel hadoop jobs.
	 * 
	 * @param defaultConf The default configuration to use
	 * @return The running job object for the completed job.
	 * @throws IOException
	 */
	public RunningJob run( Configuration defaultConf ) throws IOException
	{
		/** Construct the JobConf object from the passed in object.
		 * Ensure that the archive that contains this class will be provided to the map and reduce tasks.
		 */
	    JobConf conf = new JobConf( defaultConf, this.getClass() );
	    /**
	     * Set job specific parameters on the conf object.
	     *
	     */
	    conf.set( "our.parameter", "our.value" );
	    
	    RunningJob job = JobClient.runJob(conf);
	    return job;
	}
}
