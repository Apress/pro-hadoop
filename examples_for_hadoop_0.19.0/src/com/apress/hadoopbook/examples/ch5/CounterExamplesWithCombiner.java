/**
 * 
 */
package com.apress.hadoopbook.examples.ch5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;
import com.apress.hadoopbook.utils.Utils;


/** This class demonstrates the Reducer class counter values are incorrect if the Reducer class
 * is also used as a Combiner, and special care is not taken to make the counter names different.
 * 
 * The NaiveReducer counter values will be the sum of the Combiner calls and the Reducer calls.
 * 
 * @author Jason
 *
 */
public class CounterExamplesWithCombiner extends Configured implements Tool {

	static class CountingIdentityReducer extends
			ExamplesMapReduceBase implements
			Reducer<Text,Text,Text,Text> {

		/* (non-Javadoc)
		 * @see com.apress.hadoopbook.utils.ExamplesMapReduceBase#close()
		 */
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			super.close();
			if (reporter!=null) {
				reporter.incrCounter(taskId.isMap()?"Combiner":"Reducer", "Close", 1);
			}
		}

		/* (non-Javadoc)
		 * @see com.apress.hadoopbook.utils.ExamplesMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
		 */
		@Override
		public void configure(JobConf job) {
			// TODO Auto-generated method stub
			super.configure(job);
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			this.reporter = reporter;
			
			reporter.incrCounter("NaiveReducer", "Input Keys", 1);
			
			reporter.incrCounter(taskId.isMap() ? "Combiner" : "Reducer", "Input Keys", 1 );
			while (values.hasNext()) {
				reporter.incrCounter("NaiveReducer", "Input Value", 1);
				reporter.incrCounter(taskId.isMap() ? "Combiner" : "Reducer", "Input Values", 1 );
				output.collect( key, values.next());
			}
		}

	}


	/**
	 * @author Jason
	 *
	 */
	static class CountingIdentityMapper extends ExamplesMapReduceBase implements
			Mapper<Text, Text, Text, Text> {

	


		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			reporter.incrCounter("Map", "Input Count", 1);
			output.collect(key, value);
			
		}

	}
	


	/**
	 * 
	 */
	public CounterExamplesWithCombiner() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public CounterExamplesWithCombiner(Configuration conf) {
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
	    job.setMapperClass(CountingIdentityMapper.class);
	    job.setReducerClass(CountingIdentityReducer.class);
	    job.setCombinerClass(CountingIdentityReducer.class);
	    job.setInputFormat(KeyValueTextInputFormat.class);
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
		int res = ToolRunner.run(new Configuration(), new CounterExamplesWithCombiner(), args);
		System.exit(res);
	}


}
