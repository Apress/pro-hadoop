/**
 * 
 */
package com.apress.hadoopbook.examples.advancedtechniques;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.MainProgrameShell;
import com.apress.hadoopbook.utils.Utils;

/** A class to exercise JVM Reuse and verify that static pinned variables persist between re-uses of a JVM.
 * This class requires that the cluster be setup for JVM resuse.
 * @author Jason
 *
 */
public class JVMReuseAndStaticInitializers extends MainProgrameShell {
	/** This class just keeps a count of the number of times the singleton was gotten through the factory. */
	
	public static class Singleton {
		/** Our singleton. */
		public static Singleton singleton = null;
		/** counter of the number of times the singleton has been requested. */
		static AtomicLong getCounter = new AtomicLong(0);
		static {
			/** Construct the singleton here since we know we are going to us it. */
			singleton = new Singleton();
		}
		protected Singleton() {
		}
		/** Get an instance of the singleton, and increment our static counter. *
		 * 
		 * @return the singleton
		 */
		public static Singleton getSingleton() {
			getCounter.incrementAndGet();
			return singleton;
		}
		
		/** Return the number of times the singleton has been requested. */
		public long getCurrentCounter() {
			return getCounter.get();
		}
		
	}
	/**
	 * @author Jason

	 *
	 */
	public static class TestMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, LongWritable> {
		
		Singleton singleton;
		String taskName;
		TaskAttemptID taskId;
		
		/** grab our singleton and the taskid. */
		@Override
		public void configure(JobConf conf) {
			taskName = conf.getJobName();
			taskId = TaskAttemptID.forName(conf.get("mapred.task.id"));
			if (taskName == null || taskName.length() == 0) {
				/** if the job name is essentially unset make something up. */
				taskName = taskId.isMap() ? "map." : "reduce."
						+ this.getClass().getName();
			}

			singleton = Singleton.getSingleton();
			
		}

		/** assume there is only 1 key per map, and report on information about the singleton state. */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			/** Our key is the line number, which should be 1, and the single line, which should be the file name. */
			try {
				reporter.incrCounter("Input", "Total", 1);
				
				long initCount = singleton.getCurrentCounter();
				
				reporter.incrCounter("Singleton", taskId.toString(), initCount);
				reporter.incrCounter("Tasks", taskId.toString(), 1);
				key.set(initCount);
				output.collect(value, key);
				reporter.incrCounter("Output", "Total", 1);
				reporter.incrCounter(ManagementFactory.getRuntimeMXBean().getName(), taskId.toString(), 1);

			} catch (Throwable e) {
				reporter.incrCounter("Exceptions", "Total", 1);
				reporter.incrCounter("Exceptions", e.getClass().getName(), 1);
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				}
				if (e instanceof IOException) {
					throw (IOException) e;
				}
				throw new IOException(e);
			}
		}

	}

	Logger LOG = Logger.getLogger(JVMReuseAndStaticInitializers.class);
	
	/** This is where derived classes define required setup. 
	 * @throws IOException 
	 */
	@Override
	protected
	void customSetup(JobConf conf) throws IOException {
		super.customSetup(conf);
		if (conf.getNumTasksToExecutePerJvm()==1) {
			LOG.error("This test may only be run with JVM reuse enabled, currently the reuse count is " + conf.getNumTasksToExecutePerJvm());
			throw new IllegalArgumentException("JVM reuse not enabled");
		}
		/** Setup for NLineInputFormat, which has LongWritable, Text key, values, and only 1 input line per map. */
		conf.setInputFormat(NLineInputFormat.class);
		conf.setInt("mapred.line.input.format.linespermap",1);
		
		/** Work out how many map slots there are in the cluster. */
		JobClient client = new JobClient(conf);
		ClusterStatus status = client.getClusterStatus();
		int mapSlots = status.getMaxMapTasks();
		if (verbose) { LOG.info("There are " + mapSlots + " map execution slots in this cluster, setting up for " + mapSlots * 6 + "inputs"); }
		/** Setup our input so we have 6 input records per map slot. */
		Path root = new Path("JVMReuseAndStaticInitializers");
		Utils.makeSampleInputIf(conf, root.toString(), mapSlots*6);
		FileInputFormat.setInputPaths(conf, root);
		conf.setMapperClass(TestMapper.class);
		
		/** Ensure that each map is run one time only, to avoid biasing the singleton counts per taskid. */
		conf.setMapSpeculativeExecution(false);
		conf.setMaxMapAttempts(1);
		
		/** Setup some jvm reuse. */
		conf.setNumTasksToExecutePerJvm(6);
		FileOutputFormat.setOutputPath(conf, root.suffix(".output"));
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setNumReduceTasks(0);
		
		conf.set("mapred.child.java.opts", "-Xmx200m -verbose:class");
		
		
	}
	


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new JVMReuseAndStaticInitializers(), args);
		System.exit(res);
	}

}
