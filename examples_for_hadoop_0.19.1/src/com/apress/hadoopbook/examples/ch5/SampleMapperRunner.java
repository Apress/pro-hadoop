/**
 * 
 */
package com.apress.hadoopbook.examples.ch5;

import java.io.File;
import java.io.IOException;
import java.util.Formatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;

import com.apress.hadoopbook.utils.SpringUtils;
import com.apress.hadoopbook.utils.Utils;

/**
 * Simple class to provide a run framework around the Sample Mapper class This
 * class provides a main method that essentially understands the standard hadoop
 * arguments as defined in
 * 
 * @see org.apache.hadoop.util.GenericOptionsParser.
 * 
 * @author Jason
 * 
 */

public class SampleMapperRunner extends Configured implements Tool {
	public static Logger LOG = Logger.getLogger(SampleMapperRunner.class);
	




	/**
	 * Sample Mapper shell showing various practices
	 * 
	 * K1 and V1 will be defined by the InputFormat. K2 and V2 will be the
	 * {@link JobConf#getOutputKeyClass()} and
	 * {@link JobConf#getOutputValueClass()}, which by default are LongWritable
	 * and Text. K1 and V1 may be explicitly set via
	 * {@link JobConf#setMapOutputKeyClass(Class)} and
	 * {@link JobConf#setMapOutputValueClass(Class)}. If K1 and V1 are
	 * explicitly set, they become the K1 and V1 for the Reducer.
	 * 
	 * @author Jason
	 * 
	 */
	public static class SampleMapper<K1, V1, K2, V2> extends MapReduceBase
			implements Mapper<K1, V1, K2, V2> {
		
		/** Variable to be initialized by spring. */
		String springSetString;
		/** Variable to be initialized by spring. */
		int springSetInt;
		/** Simple getter for spring set variable
		 * @return the springSetString
		 */
		public String getSpringSetString() {
			return springSetString;
		}
		/** Setter for spring to use
		 * @param springSetString the springSetString to set
		 */
		public void setSpringSetString(String springSetString) {
			this.springSetString = springSetString;
		}
		/** Simple Getter.
		 * @return the springSetInt
		 */
		public int getSpringSetInt() {
			return springSetInt;
		}
		/** Setter for spring to use
		 * @param springSetInt the springSetInt to set
		 */
		public void setSpringSetInt(int springSetInt) {
			this.springSetInt = springSetInt;
		}


		/**
		 * Create a logging object or you will never know what happened in your
		 * task.
		 */

		/** Used in metrics reporting. */
		String taskName = null;
		/**
		 * Always save one of these away, they are so handy for almost any
		 * interaction with the framework.
		 */
		JobConf conf = null;
		/**
		 * These are nice to save, but require a test or a set each pass through
		 * the map method.
		 */
		Reporter reporter = null;
		/** Take this early, it is handy to have. */
		TaskAttemptID taskId = null;

		/**
		 * If we are constructing new keys or values for the output, it is a
		 * best practice to generate the key and value object once, and reset
		 * them each time. Remember that the map method is an inner loop that
		 * may be called millions of times. These really can't be used with out
		 * knowing an actual type
		 */
		K2 outputKey = null;
		V2 outputValue = null;
		/** The spring application context. */
		ApplicationContext applicationContext = null;
		


		/** Sample Configure methd for a map/reduce class.
		 * This method assumes the class derives from {@link MapReduceBase} 
		 * and saves a copy of the JobConf object, the taskName and the taskId into member variables.
		 * 
		 * It also will springAutoWire the mapper class
		 * and makes an instance of the output key and output value objects as member variables for the
		 * map or reduce to use.
		 * 
		 * If this method fails the Tasktracker will abort this task.
		 * @param job The Localized JobConf object for this task
		 */
		@SuppressWarnings("unchecked")
		public void configure(JobConf job) {
			super.configure(job);
			LOG.info("Map Task Configure");
			this.conf = job;
			try {
				taskName = conf.getJobName();
				taskId = TaskAttemptID.forName(conf.get("mapred.task.id"));
				if (taskName == null || taskName.length() == 0) {
					/** if the job name is essentially unset make something up. */
					taskName = taskId.isMap() ? "map." : "reduce."
							+ this.getClass().getName();
				}
				springAutoWire(job);
				
				
				/**
				 * These casts are safe as they are checked by the framework
				 * earlier in the process.
				 */
				outputKey = (K2) conf.getMapOutputKeyClass().newInstance();
				outputValue = (V2) conf.getMapOutputValueClass().newInstance();
			} catch (RuntimeException e) {
				/** Try to dump the class path and the load path, and the contents of the cache directory if this failed. */
				StringBuilder logMsg = new StringBuilder();
				Formatter fmt = new Formatter(logMsg);
				fmt.format( "%s%n", "Task current working directory contents%n");
				fmt.format( "Task failed to initialize, classpath %s, library load path %s%n",
						System.getProperty("java.class.path"), System.getProperty("java.library.path"));
				
				File cwd = new File(".");
				File contents[] = cwd.listFiles();
				if (contents!=null) {
					for( File item : contents ) {
						fmt.format( "\t%s, a %s of size %d%n", item.getName(), item.isFile() ? "file" : "directory", item.length());
					}
				} else {
					fmt.format( "\t empty%n");
				}
				fmt.flush();
				LOG.error(logMsg,e);
				
				throw e;
			} catch (InstantiationException e) {
				LOG.error(
						"Failed to instantiate the key or output value class",
						e);
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				LOG
						.error(
								"Failed to run no argument constructor for key or output value objects",
								e);
				throw new RuntimeException(e);
			}
			LOG.info(taskId.isMap() ? "Map" : "Reduce" + " Task Configure complete");

		}
		/** Handle spring configuration for the mapper.
		 * The bean definition has to be <code>lazy-init="true"</code> as this object must be initialized.
		 * This will fail if Spring Weaves a wrapper class for AOP around the configure bean.
		 * 
		 * The bean name is extracted from the configuration as mapper.bean.name or reducer.bean.name
		 * or defaults to taskName.XXXX.bean.name
		-+-+-+-+-+-+-+-+-+-+-+-+-+ * 
		 * The application context is loaded from mapper.bean.context or reducer.bean.context and may be a set of files
		 * The default is jobName.XXX.bean.context
		 * 
		 * @param job
		 */
		 void springAutoWire(JobConf job) {
			String springBaseName = taskId.isMap()? "mapper.bean": "reducer.bean";
			
			/** Construct a bean name for this class using the configuration or a default name. */
			String beanName = conf.get(springBaseName + ".name",
				taskName + "." + springBaseName + ".name" );
			LOG.info("Bean name is " + beanName);
			applicationContext = SpringUtils.initSpring(job, springBaseName + ".context", springBaseName + ".context.xml");
			if (applicationContext==null) {
				throw new RuntimeException("Unable to initialize spring configuration for " + springBaseName);
			}
			AutowireCapableBeanFactory autowire = applicationContext.getAutowireCapableBeanFactory();
			Object mayBeWrapped = autowire.configureBean( this, beanName);
			if (mayBeWrapped != this) {
				throw new RuntimeException( "Spring wrapped our class for " + beanName);
			}
		}

		/** Sample close method that sets the task status based on how many map exceptions there were.
		 * This assumes that the reporter object passed into the map method was saved and
		 *  that the JobConf object passed into the configure method was saved.
		 */
		public void close() throws IOException {
			super.close();
			LOG.info("Map task close");
			if (reporter != null) {
				/**
				 * If we have a reporter we can perform simple checks on the
				 * completion status and set a status message for this task.
				 */
				Counter mapExceptionCounter = reporter.getCounter(taskName,
						"Total Map Failures");
				Counter mapTotalKeys = reporter.getCounter(taskName,
						"Total Map Keys");
				if (mapExceptionCounter.getCounter() == mapTotalKeys
						.getCounter()) {
					reporter.setStatus("Total Failure");
				} else if (mapExceptionCounter.getCounter() != 0) {
					reporter.setStatus("Partial Success");
				} else {
					/** Use the spring set bean to show we did get the values. */
					reporter.incrCounter( taskName, getSpringSetString(), getSpringSetInt());
					reporter.setStatus("Complete Success");
				}
			}
			/**
			 * Ensure any HDFS files are closed here, to force them to be
			 * committed to HDFS.
			 */
		}

		/**
		 * The map function, this is a nested inner loop and should be as clean
		 * and simple as possible.
		 * 
		 * @param key
		 *            The key object for this record. The key contents are only
		 *            valid for the duration of the map method.
		 * @param value
		 *            THe value object for this record. The value contents are
		 *            only valid for the duration of the map method.
		 * @param output
		 *            The output collector for records that are to be output or
		 *            passed to the reducer.
		 * @param reporter
		 *            The object to report status back to the framework. This
		 *            also provides a 'I am not hung' heartbeat to the
		 *            framework.
		 * 
		 *            The framework reuses the key and value objects,
		 *            reinitializing the contents for each call to map. If the
		 *            map method is going to take more than the number of
		 *            milliseconds stored under the key mapred.task.timeout,
		 *            default 600000 (10 min) A call must be made on the
		 *            reporter object to heartbeat with the Tasktracker or the
		 *            task will be killed.
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 *      java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 *      org.apache.hadoop.mapred.Reporter)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public void map(K1 key, V1 value, OutputCollector<K2, V2> output,
				Reporter reporter) throws IOException {
			try {
				this.reporter = reporter;
				reporter.incrCounter(taskName, "Total Map Keys", 1);

				reporter.incrCounter(taskName, "Total Map Output Keys", 1);

				/** This will fail in the general case. */
				output.collect((K2) key, (V2) value);

			} catch (Throwable t) {
				reporter.incrCounter(taskName, "Total Map Failures", 1);
				reporter.incrCounter(taskName, "Map Failures: "
						+ t.getClass().getName(), 1);
				if (t instanceof IOException) {
					throw (IOException) t;
				}
				if (t instanceof RuntimeException) {
					throw (RuntimeException) t;
				}
				throw new IOException(t);
			}

		}
	}

	/**
	 * 
	 */
	public SampleMapperRunner() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public SampleMapperRunner(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}

	static public class ConcreteMapper extends
			SampleMapper<Text, Text, Text, Text> {

	};

	/**
	 * Actually run the job, set up the configuration, force our mapper and
	 * disable reduce. Takes a an optional single argument --deleteOutput in
	 * which case if the ouput directory exists and the output format is text,
	 * the directory will be deleted before the job starts.
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		if (args.length > 1
				|| (args.length == 1 && !"--deleteOutput".equals(args[0]))) {
			System.err
					.println("Usage: SampleMapperRunner HadoopOptions [--deleteOutput]");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		boolean deleteOutput = args.length == 1;

		final JobConf job = new JobConf(getConf()); // Initialize the JobConf
													// object to be used for
													// this job from the command
													// line configured JobConf.
		job.setInt("io.sort.mb", 10);
		/**
		 * Small memory use for small machines, my testing laptop has only 1gig
		 * of ram
		 */

		job.setJobName("SampleMapperJob");
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);

		job.setMapperClass(ConcreteMapper.class);
		job.setJarByClass(SampleMapper.class);
		job.setNumReduceTasks(0);
		/**
		 * if the input format is textual and an input file has not been
		 * specified,make one with some random data.
		 */
		Utils.makeSampleInputIf(job, "sampleMapperInput", 2);
		/**
		 * If the output path is not set and it is text, set it to something
		 * reasonable.
		 */
		Utils.setupAndRemoveOutputIf(job, "sampleMapperOutput", deleteOutput);

		/** Send the job to the framework. */
		RunningJob rj = launch(job);
		Counters counters = rj.getCounters();
		System.out.println("The Job is "
				+ (rj.isComplete() ? " complete " : " incomplete")
				+ (rj.isComplete() ? " and "
						+ (rj.isSuccessful() ? " successfull " : " failed ")
						: ""));

		for (Counters.Group group : counters) {
			System.out.println("Counter Group: " + group.getDisplayName());
			for (Counters.Counter counter : group) {
				System.out.println("\t" + counter.getDisplayName() + "\t"
						+ counter.getCounter());
			}
		}
		return 0;
	}

	/**
	 * This is a stylized method that the Tool pattern requires.
	 * 
	 * @param job
	 *            The {@link JobConf} object to use to launch the job.
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
		int res = ToolRunner.run(new Configuration(), new SampleMapperRunner(),
				args);
		System.exit(res);
	}

}
