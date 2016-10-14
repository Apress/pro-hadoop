/**
 * 
 */
package com.apress.hadoopbook.utils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.log4j.Logger;

/**
 * @author Jason
 *
 */
public class ExamplesMapReduceBase extends MapReduceBase {
	static Logger LOG = Logger.getLogger(ExamplesMapReduceBase.class);
		
		
		/** Finalize, to record event in the off chance the finalize method is called. @see Object#finalize.
		@Override
		protected void finalize() {
			events.add(new Event( hashCode(), myId == null ? "" : myId, "finalize"));
		}
		
		
		/** The archived job conf object. 
		 */
		protected JobConf conf = null;
		
	
		/** The task name */
		protected String taskName;
		/** The task id */
		protected TaskAttemptID taskId;
		
		/** The jvm name. */
		static String jvmId = ManagementFactory.getRuntimeMXBean().getName();
		
		/** Saved instance of the {@link Reporter} to use in the close method. */
		protected Reporter reporter;
		/** The class path archives passed via the distributed cache. */
		List<Path> classPathArchives;
		/** The class path files passed via the distributed cache. */
		List<Path>  classPathFiles;

		/** The non class path archives passed via the distributed cache. */
		List<Path>  nonClassPathArchives;
		List<Path>  nonClassPathFiles;

		
		/** Our standard config method, really should refactor much of this into a base class.
		 * 
		 * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
		 */
		@Override
		public
		void configure(JobConf job)
		{
			if (job==null) {	/** It is easy to make a mistake and pass conf instead of job. */
				throw new NullPointerException("Null job object passed to configure");
			}
			super.configure(job);
			conf = job;

			taskName = conf.getJobName();
			taskId = TaskAttemptID.forName(conf.get("mapred.task.id"));
			if (taskName == null || taskName.length() == 0) {
				/** if the job name is essentially unset make something up. */
				taskName = taskId.isMap() ? "map." : "reduce."
						+ this.getClass().getName();
			}
			LOG.info( "Task " + taskName + ": " + taskId.getId() + " " + jvmId + " Log Dir " + TaskLog.getTaskLogFile(taskId, LogName.STDERR).getParent());

			classPathArchives = arrayToList(DistributedCache.getArchiveClassPaths(conf));
			classPathFiles = arrayToList(DistributedCache.getFileClassPaths(conf));
			try {
				nonClassPathArchives = arrayToList(DistributedCache.getLocalCacheArchives(conf));
				nonClassPathFiles = arrayToList(DistributedCache.getLocalCacheFiles(conf));
			} catch( IOException ignore ) {
				LOG.error( "Exception extracting cache files or archives from distributed cache, ignored", ignore);
			}
		}

		
		/** Just record the event
		 * @see org.apache.hadoop.mapred.MapReduceBase#close()
		 */
		@Override
		public void close() throws IOException
		{
			super.close();

		}


		/** This method takes an array of items, that may be null and returns a List of the items.
		 * 
		 * @param array The array of objects to convert to a list. null is okay
		 * @return the list of items, the Collections empty list is returned for a null array.
		 */
		public static <T> List<T> arrayToList( T[] array ) {
			if (array==null) {
				return Collections.<T>emptyList();
			}
			return Arrays.asList(array);
		}

		/** Handy routine to translate exceptions into IOExceptions and log to the counters.
		 * A {@link #LOG} message at level error is logged.
		 * 
		 * @param reporter The reporter object to use. If null no reporting is done.
		 * @param message Message to use, in the log message, if not null.
		 * @param e The exception that was caught
		 * @throws IOException e directly or as a cause of a new {@link IOException}.
		 */
		public void throwsIOExcepction( Reporter reporter, String message, Throwable e) throws IOException {
		
			if (reporter!=null) {
				reporter.incrCounter(taskName, getType() + "ExceptionsTotal", 1);
				reporter.incrCounter(taskName, getType() + "Exceptions." + e.getClass().getName(), 1);
			}
			if (message!=null) {
				LOG.error( message, e);
			} else {
				LOG.error(e);
			}
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			if (e instanceof IOException) {
				throw (IOException) e;
			}
			throw new IOException(e);

		}


		/**
		 * @return
		 */
		public String getType() {
			return (taskId.isMap() ? "Map" :  "Reduce");
		}

	
	}
