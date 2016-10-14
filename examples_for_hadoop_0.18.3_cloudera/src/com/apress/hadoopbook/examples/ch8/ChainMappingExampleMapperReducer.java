
package com.apress.hadoopbook.examples.ch8;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.log4j.Logger;

/** Class to provide a mapper and a reducer that will log the ordering of method calls.
 * This helps to trace the actions that take place in the Chain Mapping Example
 * 
 * @author Jason
 *
 */
class ChainMappingExampleMapperReducer extends MapReduceBase implements
Mapper<Text, Text, Text, Text>, Reducer<Text,Text,Text,Text> {
	static Logger LOG = Logger.getLogger(ChainMappingExampleMapperReducer.class);
	
	/** This logs each event to the {@link #LOG} and archives the event in the {@link events} list
	 * as an aid to monitoring in the debugger.
	 * 
	 * @author Jason
	 *
	 */
	/**
	 * @author Jason
	 *
	 */
	static class Event {
		/** Used to generate sequence numbers for events. */
		static AtomicLong sequence = new AtomicLong(0);
		/** Descriptive string about the event. */
		final String what;
		/** The id for this task, this is not available before the {@link #configure(JobConf)} method. */
		final String myId;
		/** The object's hash code, to tie together events that happen before {@link #myId} is available. */
		final int objectHash;
		/** The sequence number for this event. */
		final long seq;
		
		
		/** public method to generate and record and event.
		 *  
		 * @param objectHash The object hash code of the object generating the event, for objects that don't have {@link #myId} yet.
		 * @param myId The id for the object, may be "".
		 * @param what Generic descriptive string for the event.
		 * @return
		 */
		static public Event recordEvent( int objectHash, final String myId, final String what ) {
			return new Event(  objectHash,  myId,  what );
		}
		
		/** protected constructor to generate and record and event.
		 *  
		 * @param objectHash The object hash code of the object generating the event, for objects that don't have {@link #myId} yet.
		 * @param myId The id for the object, may be "".
		 * @param what Generic descriptive string for the event.
		 */
		protected Event( int objectHash, final String myId, final String what ) {
			this.objectHash = objectHash;
			this.myId = myId;
			this.what = what;
			seq = sequence.getAndIncrement();
			LOG.info( "Event " + seq + " " + myId + " " + objectHash + " " +  what);
			synchronized (events) {
				events.add(this);
			}
		}
		/** The list of events. This grows without bounds. */
		static ArrayList<Event> events = new ArrayList<Event>();
	};
	
	
	/** No arg constructor to record constructor event. */
	public ChainMappingExampleMapperReducer() {
		Event.recordEvent( hashCode(), "", "constructor" );
	}
	
	/** Finalize, to record event in the off chance the finalize method is called. @see Object#finalize.
	@Override
	protected void finalize() {
		events.add(new Event( hashCode(), myId == null ? "" : myId, "finalize"));
	}
	
	
	/** The for this mapper/reducer from the configuration key {@link #idLabel} */
	protected String myId = null;
	/** The archived job conf object. 
	 */
	protected JobConf conf = null;
	
	/** Used for producing a modified text key, with this item's id. */
	protected Text alteredValue = new Text();
	
	/** The task name */
	protected String taskName;
	/** The task id */
	TaskAttemptID taskId;
	
	/** The jvm name. */
	static String jvmId = ManagementFactory.getRuntimeMXBean().getName();
	
	/** Saved instance of the {@link OutputCollector} to use in the close method. */
	OutputCollector<Text,Text> output;
	/** Saved instance of the {@link Reporter} to use in the close method. */
	Reporter reporter;

	
	/** Our standard config method, really should refactor much of this into a base class.
	 * 
	 * @see org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	public
	void configure(JobConf job)
	{
		super.configure(job);
		conf = job;
		myId = conf.get(ChainMappingExample.idLabel, "unknown" );
		taskName = conf.getJobName();
		taskId = TaskAttemptID.forName(conf.get("mapred.task.id"));
		if (taskName == null || taskName.length() == 0) {
			/** if the job name is essentially unset make something up. */
			taskName = taskId.isMap() ? "map." : "reduce."
					+ this.getClass().getName();
		}
		LOG.info( "Task " + taskName + ": " + taskId.getId() + " " + jvmId + " Log Dir " + TaskLog.getTaskLogFile(taskId, LogName.STDERR).getParent());
		Event.recordEvent( hashCode(), myId, "configure");
	}

	
	/** Just record the event
	 * @see org.apache.hadoop.mapred.MapReduceBase#close()
	 */
	@Override
	public void close() throws IOException
	{
		super.close();
		Event.recordEvent( hashCode(), myId, "close");
	}

	
	/** Our map, just pass through any key value pairs received, and log an event about them.
	 * 
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			Event.recordEvent( hashCode(), myId, "map " + key);
			reporter.incrCounter(myId, "MapInput", 1 );
			this.output = output;
			this.reporter = reporter;
			output.collect( key, value);	
			
			reporter.incrCounter(myId, "MapOutput", 1 );
		} catch (Throwable e) {
			reporter.incrCounter(myId, "MapExceptionsTotal", 1);
			e.printStackTrace(System.out);
			reporter.incrCounter(myId, "MapExceptions." + e.getClass().getName(), 1);
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			if (e instanceof IOException) {
				throw (IOException) e;
			}
			throw new IOException(e);
		}
		
	}

	/** Our reduce pass through individual key, value pairs for each key value set received, log an event for the method call.
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			Event.recordEvent( hashCode(), myId, "reduce " + key);
			reporter.incrCounter(myId, "ReduceInput", 1 );
			this.output = output;
			this.reporter = reporter;
			while (values.hasNext()) {
				Text value = values.next();
			
				output.collect( key, value);	
				
				reporter.incrCounter(myId, "ReduceOutput", 1 );
			}
		} catch (Throwable e) {
			reporter.incrCounter(myId, "ReduceExceptionsTotal", 1);
			reporter.incrCounter(myId, "ReduceExceptions." + e.getClass().getName(), 1);
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
