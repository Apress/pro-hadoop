/**
 * 
 */
package com.apress.hadoopbook.examples.ch5;

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


/** Simple class to demonstrate the programatic interaction with the JobCounters/
 * @author Jason
 *
 */
public class CounterExamples extends Configured implements Tool {

	/**
	 * 
	 */
	public CounterExamples() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public CounterExamples(Configuration conf) {
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
	    
	    job.setOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setJobName("CounterExample");
	    
		/**
		 * if the input format is textual and an input file has not been
		 * specified,make one with some random data.
		 */
		Utils.makeSampleInputIf(job, job.getJobName() + ".input", 2);
		/**
		 * If the output path is not set and it is text, set it to something
		 * reasonable.
		 */
		Utils.setupAndRemoveOutputIf(job, job.getJobName() + ".ouput", deleteOutput);
	    
	    
	    /** Send the job to the framework. */
	    RunningJob rj = launch(job);
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
	  
	  /** This is a stylized method that the Tool pattern requires.
	   * @param job The {@link JobConf} object to use to launch the job.
	   */
	protected RunningJob launch(JobConf job) throws IOException {
		JobConf conf = new JobConf(job, this.getClass());
		
		RunningJob rj = JobClient.runJob(conf);
		return rj;
		
		
	}


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CounterExamples(), args);
		System.exit(res);
	}


}
