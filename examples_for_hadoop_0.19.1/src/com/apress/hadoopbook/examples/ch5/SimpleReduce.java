/**
 * 
 */
package com.apress.hadoopbook.examples.ch5;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.GroupByLongGroupingComparator;
import com.apress.hadoopbook.utils.Utils;

/** Simple class to demonstrate a minimal reduce.
 * 
 * @author Jason
 *
 */
public class SimpleReduce extends Configured implements Tool {

	/**
	 * 
	 */
	public SimpleReduce() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public SimpleReduce(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	
	/** Actually parse the arguments and setup the job.
	 * The class {@link org.apache.hadoop.util.GenericOptionsParser GenericOptionsParser}
	 * handles the hadoop standard arguments.
	 * 
	 * @param args The unprocessed Arguments.
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	  public int run(String[] args) throws Exception {
		  if (args.length > 1
				  || (args.length == 1 && !"--noDeleteOutput".equals(args[0]))) {
			  System.err
			  .println("Usage: SimpleReduce HadoopOptions [--noDeleteOutput]");
			  ToolRunner.printGenericCommandUsage(System.err);
			  return -1;
		  }
		  boolean deleteOutput = args.length != 1;
		  final JobConf job = new JobConf(getConf()); // Initialize the JobConf object to be used for this job from the command line configured JobConf.
		  job.setInt("io.sort.mb", 10);/** Small memory use for small machines, my testing laptop has only 1gig of ram */

		  job.setJobName("SampleReduce");
		  
		  Path inputDir = new Path( job.getJobName() + ".input");
		  setupInput(job, inputDir);
		  job.setInputFormat(KeyValueTextInputFormat.class);
		  FileInputFormat.setInputPaths(job, inputDir);
		  
		  job.setMapperClass(SimpleReduceTransformingMapper.class);
		  job.setMapOutputValueClass(LongWritable.class);
		  job.setMapOutputKeyClass(LongWritable.class);
		  
		  /** Force the reduce to take text as the output value class, instead of the default. */
		  job.setOutputValueClass(Text.class);
		  job.setOutputKeyClass(Text.class);
		  job.setReducerClass(SimpleReduceTransformingReducer.class);
		  
		  /** Cause the keys to be grouped by 10s. */
		  job.setOutputValueGroupingComparator(GroupByLongGroupingComparator.class);
		  job.setNumReduceTasks(1);		/** Ensure that all keys go to 1 reduce so the group by is stable. */
		  
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

	/** Generate some input files for the SimpleReduce job.
	 * The input files are composed of records of 2 numbers, the first a sequence number the second a random positive integer.
	 * The input lines are generated in random order.
	 * 
	 * If the input directory exists, nothing is done.
	 * 
	 * @param conf The Configuration object to work on
	 * @param inputDir the directory to use.
	 * @throws IOException
	 */
	public static void setupInput(final Configuration conf, final Path inputDir) throws IOException {
		FileSystem fs = inputDir.getFileSystem(conf);
		/** If inputDir exists, do nothing. */
		  if (fs.exists(inputDir)) {
			  return;
		  }
		  if (!fs.mkdirs(inputDir)) {
			  throw new IOException( "Unable to make input directory " + inputDir);
		  }
		  SampleOutputGenerator creator = new SampleOutputGenerator( conf, new Path( inputDir, "input") );
		  creator.generateDataFile(conf, 0, 100);
		  creator.setWhich(1);
		  creator.generateDataFile(conf, 100, 100);
		  creator.setWhich(2);
		  creator.generateDataFile(conf, 150, 100);
		  creator.setWhich(3);
		  creator.generateDataFile(conf, 200, 100);
		  creator.setWhich(4);
		  creator.generateDataFile(conf, 250, 100);
	}
	  
	  /** This is a stylized method that the Tool pattern requires.
	   * @param job The {@link JobConf} object to use to launch the job.
	   */
	protected RunningJob launch(JobConf job) throws IOException {
		JobConf conf = new JobConf(job, this.getClass());
		
		RunningJob rj = JobClient.runJob(conf);
		return rj;
		
		
	}

	/** Main method run -help for usage.
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SimpleReduce(), args);
		System.exit(res);

	}

}
