/**
 * 
 */
package com.apress.hadoopbook.examples.ch7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.Utils;


/** Simple class to demonstrate the programatic interaction with the JobCounters, modified to make unit testing simpler
 * 
 * @author Jason
 *
 */
public class TestableCounterExamples extends Configured implements Tool {

	/**
	 * 
	 */
	public TestableCounterExamples() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public TestableCounterExamples(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	  public int run(String[] args) throws Exception {
			if (args.length > 1
					|| (args.length == 1 && !"--deleteOutput".equals(args[0]))) {
				System.err
						.println("Usage: CounterExamples HadoopOptions [--deleteOutput]");
				ToolRunner.printGenericCommandUsage(System.err);
				return -1;
			}
			boolean deleteOutput = args.length == 1;
	    final JobConf job = new JobConf(getConf()); // Initialize the JobConf object to be used for this job from the command line configured JobConf.
	    job.setInt("io.sort.mb", 10);/** Small memory use for small machines, my testing laptop has only 1gig of ram */


	    /** Send the job to the framework. */
	    RunningJob rj = launch(job, deleteOutput);
	    Counters counters = rj.getCounters();
	    System.out.println( "The Job is " 
	    		+ (rj.isComplete() ? " complete " : " incomplete")
	    		+ (rj.isComplete() ? " and " + (rj.isSuccessful() ? " successfull " : " failed ") : "")
	    		);
	    		
	    for( Counters.Group group : counters) {
	    	System.out.println( "Counter Group: " + group.getDisplayName());
	    	for( Counters.Counter counter: group) {
	    		System.out.println( "\t" + counter.getDisplayName() + "\t" + counter.getCounter());
	    	}
	    }
		return 0;
	  }
	  
	  /** This is a stylized method that the Tool pattern requires. It is structured to allow unit testing.
	   * @param job The {@link JobConf} object to use to launch the job.
	   */
	protected RunningJob launch(JobConf job, boolean deleteOutput) throws IOException {
		JobConf conf = new JobConf(job, this.getClass());
	    conf.setJobName("TestableCounterExample");
	    
	    if (conf.get("mapred.output.value.class")==null) {
	    	conf.setOutputValueClass(Text.class);
	    }
	    if (conf.get("mapred.output.key.class")==null) {
	    	conf.setOutputKeyClass(Text.class);
	    }
	    
		/**
		 * if the input format is textual and an input file has not been
		 * specified,make one with some random data.
		 */
		Utils.makeSampleInputIf(conf, job.getJobName() + ".input", 2);
		/**
		 * If the output path is not set and it is text, set it to something
		 * reasonable.
		 */
		Utils.setupAndRemoveOutputIf(conf, conf.getJobName() + ".ouput", deleteOutput);
	    
	    
		
		RunningJob rj = JobClient.runJob(conf);
		return rj;
		
		
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TestableCounterExamples(), args);
		System.exit(res);
	}


}
