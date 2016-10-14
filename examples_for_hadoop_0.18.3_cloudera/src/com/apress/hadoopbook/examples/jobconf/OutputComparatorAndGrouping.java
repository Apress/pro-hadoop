/**Simple class to demonstrate how output comparators and output value grouping works.
 * 
 */
package com.apress.hadoopbook.examples.jobconf;

import java.io.IOException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Demonstrate the use of an output partitioner and an output value grouper.
 * 
 * This class produces a series of keys of the form <code>Reduce # ItemNo</code>.
 * The output partitioner selects the partition based on the # portion of the Reduce, 
 * and the output value grouping groups the keys in sets of 5 based on the numeric value of ItemNo.
 * 
 * An ItemNo pair are considered equal by the grouping comparator if the quotient 
 * of their division by the grouping factors, ignoring the remainder are equal.
 * 
 * Our standard example uses 5 as the grouping, so 21/5 = 4 and 24/5 = 4 are considered equal.
 * 
 * The MapOutputComparator is the standard comparator that uses the entire key for sorting, which ensures that we will see the keys in order.
 * 
 * The input data is in a random order to ensure that the sort demonstration works by design rather than by accident.
 * @author Jason
 *
 */
public class OutputComparatorAndGrouping extends Configured implements Tool {
	static final Log LOG = LogFactory.getLog(OutputComparatorAndGrouping.class);

	/** Stub constructor to allow object construction.
	 * 
	 */
	public OutputComparatorAndGrouping() {
		// Stub for the framework
		
	}
	
	/** More framework required constructors.
	 * @param conf The configuration object to use as a base for configuration information.
	 */
	public OutputComparatorAndGrouping(Configuration conf) {
		super(conf);
	}
	
	
	/** The value grouping comparator. 
	 * This determines if two keys are to be considered equal at the time that the key and value groups are being
	 * fed to the {@link Reducer#reduce(Object, Iterator, OutputCollector, Reporter)} method.
	 * The keys are expected to be sorted by the {@link JobConf#getOutputKeyComparator()} such that all of the
	 * keys that would be considered equal by this class are adjacent.
	 * 
	 *  This particular class extracts a numerical field from the keys,
	 *  and if the resulting integer quotient is equal after division by the {@link #groupingFactor},
	 *  the keys are considered equal. 
	 * 
	 * @author Jason
	 *
	 */
	static class ValueGroupingComparator extends WritableComparator {

		/** Framework required constructors. */
		public ValueGroupingComparator() {
			super(Text.class);
		}
		
		/** Framework required constructors. */
		protected ValueGroupingComparator(
				Class<? extends WritableComparable<Text>> keyClass) {
			super(keyClass);
		}

		/** Framework required constructors. */
		protected ValueGroupingComparator(Class<? extends WritableComparable<Text>> keyClass,
			      boolean createInstances) {
			 super(keyClass, createInstances);
			 
		 }
		 /** Use this as the divisor to determine the quotient of a key's specified numeric field.
		  * 20/5 = 4, 21/5 = 4, 24/5 = 4, 25/5 = 5.
		  */
		int groupingFactor = 5;
		
		/** Group the keys by the grouping factor. in this case for our numeric field
		 * value 0->4 is in the first group, 5-9 in the second group and so on.
		 * @param leftNumber The left hand side value to compare
		 * @param rightNumber The right hand side value to compare
		 * @return
		 */
		int compareGroups( int leftNumber, int rightNumber ) {
			leftNumber /= groupingFactor;
			rightNumber /= groupingFactor;
			if (leftNumber < rightNumber ) {
				return -1;
			}
			if (leftNumber > rightNumber ) {
				return 1;
			}
			return 0;
		}

		/** Look for the last whole number in the key, that has a leading space before it or is the only item in the key. */
		static Pattern valueGroupingPattern = Pattern.compile( "(?:^|\\s)(-?[0-9]+)$");
		
		/** Used to ensure that the matching is thread safe. This is probably overkill. */
		static ThreadLocal<Matcher> localMatcher = new ThreadLocal<Matcher>() {
			@Override
			/** Create an initial matcher object for our pattern {@link #valueGroupingPattern}.
			 * @return a {@link Matcher} object that can be used for searching for {@link #valueGroupingPattern}.
			 */
			protected Matcher initialValue() {
				return valueGroupingPattern.matcher("");
			}
		};
		
		/** If the keys are of the form 'something space number' we will use the number to group the keys.
		 * If not, then the natural ordering will be used.
		 * 
		 * If there are any unexpected types or data in the parameters, the default compare for the object type is performed.
		 * 
		 * @param o1 The left hand side key object to compare. This should be of type Text
		 * @param o2 The right hand side key object to compare. This should be of type Text
		 */
		@Override
		public int compare(Object o1, Object o2) {
			
			/** If the keys are not text objects use the natural comparator. */
			if (!(o1 instanceof Text && o2 instanceof Text) ) {
				LOG.error( "Can't compare grouping keys, wrong type " + o1.getClass().getName() + ", " + o2.getClass().getName());
				return super.compare( o1, o2);
			}
			String left = ((Text) o1).toString();
			String right = ((Text) o2).toString();
			
			/** Extract the number portion of the keys (the second and last field if the fields are space separated. */
			Matcher matcher = localMatcher.get();
			
			matcher.reset( left );
			if (!matcher.find()) {
				/** The key doesn't match our key pattern, use the natural comparator.*/
				LOG.error( "Can't find trailing index in key o1 " + left);
				return super.compare(o1, o2);
			}
			int leftNumber = Integer.parseInt(matcher.group(1));
			
			matcher.reset( right );
			if (!matcher.find()) {
				/** The key doesn't match our key pattern, use the natural comparator.*/
				LOG.error( "Can't find trailing index in key o2 " + right);
				return super.compare( o1, o2);
			}
			int rightNumber = Integer.parseInt(matcher.group(1));
			
			/** Call the grouping comparator with the numbers. */
			return compareGroups( leftNumber, rightNumber );
			
		}
		
	}
	
	
	/** Simple class to demonstrate how the grouping of key values works.
	 * 
	 * This class assumes there is redundant data between the key and the value, and that the key sort has been
	 * done fully, and that the data loss in the individual keys, by grouping can be extracted from the value operators.
	 * The grouping requires that all keys that should group together be placed into the same reduce partition.
	 * 
	 * In the specific case, we are looking at keys of the form "Reduce # ItemNo" and values of the form "ItemNo data"
	 * The grouping operator will cause keys the keys to be compared as (int) (ItemNo/5), so we will get up to 5 distinct keys
	 * grouped into one key.
	 * ie: keys Reduce 1 0, Reduce 1 1, Reduce 1 2, Reduce 1 3, Reduce 1 4 will group as the same key, while Reduce 1 5 will be a different key. 
	 *  
	 *  The actual output key is the Incoming Key.
	 *  The value output is the joining of Item Numbers and all of the value strings.
	 *  Note: the item numbers are extracted from the individual value strings.
	 */
	static class CountingReducer<K,V> extends MapReduceBase implements Reducer<K, V, K, Text> {

		/** This pattern looks for the first whole number after white space in a string. Our values are <code>Item #</code>... this picks up the # */
		static Pattern valueItemPattern = Pattern.compile( "\\s+(-?[0-9]+)");
		
		/** This is used many times, so create it once. */
		Text outputValue = new Text();
		/** Compute the item numbers here for easy reading. */
		StringBuilder leader = new StringBuilder();
		/** No need for multiple instances of this object. */
		StringBuilder sb = new StringBuilder();
		Matcher itemNumberMatcher = valueItemPattern.matcher("");
		/** Writes all keys and values directly to output. 
		 * @throws IOException */
		public void reduce(K key, Iterator<V> values,
				OutputCollector<K, Text> output, Reporter reporter)
		throws IOException {
			try {
				/** Clear the string builder objects that will accumulate the ItemNo's and the full value strings. */
				leader.setLength(0);
				sb.setLength(0);
				while (values.hasNext()) {
					/* For each value, we need to extract the ItemNo and also the string data. */
					String currentValue = values.next().toString();
					itemNumberMatcher.reset(currentValue);
					if (itemNumberMatcher.find()) {
						/** We found a leading number in the value, which we assume is the ItemNo for this record. */
						reporter.incrCounter("Match Stats", "Found", 1);
						/** Accumulate the item number in our string builder object. */
						leader.append(itemNumberMatcher.group(1));
						leader.append(", ");
					} else {
						/** No leading number, this is really a violation of the contract for this class. */
						reporter.incrCounter("Match Stats", "NotFound", 1);
					}
					/** Accumulate the value string in our string builder. */
					sb.append(currentValue.toString());
					sb.append(", ");
				}
				if (sb.length()>2) {
					sb.setLength( sb.length()-2);// Lose the trailing ', ' of the last append
				}
				if (leader.length()>2) {
					leader.setLength(leader.length()-2);
				}
				/** Build the string that will be the output value. */
				if (leader.length()>0) {
					leader.append( ": ");
				}
				leader.append(sb);
				outputValue.set( leader.toString() );
				output.collect( key, outputValue );
			} catch( Throwable e) {
				reporter.incrCounter("Failures", e.getClass().getName(), 1);
				if (e instanceof IOException) {
					throw (IOException) e;
				} else {
					throw new IOException("reduce failure", e);
				}
			}
		}

	}
	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	  public int run(String[] args) throws Exception {
	    if (args.length < 2) {
	      System.err.println("Usage: OutputComparatorAndGrouping startInt countInt. StartInt must be > countInt/2");
	      ToolRunner.printGenericCommandUsage(System.err);
	      return -1;
	    }
	    /** determine the starting value of our Item Numbers, and how many items to write out. */
	    final int start = Integer.parseInt(args[0]);
	    final int count = Integer.parseInt(args[1]);
	    if (start < count/2) {
	    	System.err.println("Usage: OutputVComparatorAndGrouping startInt countInt. StartInt must be > countInt/2");
	    	ToolRunner.printGenericCommandUsage(System.err);
	    	return -1;
	    }
	    final JobConf job = new JobConf(getConf()); // Initialize the JobConf object to be used for this job from the command line configured JobConf.
	    job.setInt("io.sort.mb", 10);/** Small memory use for small machines, my testing laptop has only 1gig of ram */
	    final FileSystem fs = FileSystem.get(job);
	    final Random rnd = new Random();
	    
	    /** Create some directories to use for input and output. They both must not exist. */
	    Path inputPath = new Path( this.getClass().getName() + ".input." + rnd.nextInt(Integer.MAX_VALUE));
	    Path outputPath = new Path( this.getClass().getName() + ".output." + rnd.nextInt(Integer.MAX_VALUE));
	    
	    if (fs.exists(inputPath)) {
	    	System.err.println("Unexpected input path found at " + inputPath + " remove or rerun");
	    	return -1;
	    }
	    if (fs.exists(outputPath)) {
	    	System.err.println("Unexpected output directory found at " + outputPath + " remove or rerun");
	    	return -1;
	    }
	    
	    /** Our input format for this job is text, and in fact <code>key TAB value EOL</code> based records. */
	    FileInputFormat.addInputPath( job, inputPath);
	    job.setInputFormat(KeyValueTextInputFormat.class);
	    
	    /** Just pass the values through unchanged. */
	    job.setMapperClass(IdentityMapper.class);
	    
	    /** Use our CountingReducer to make clear that we are doing grouping and aggregation at the reduce level. */
	    job.setReducerClass(CountingReducer.class);
	    
	    /** Use a sequence file output format to make reading the files back simpler, and to demonstrate how to do so. */
	    job.setOutputFormat(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setOutputPath(job, outputPath);
	    /** Our map output and reduce input and output are Text for the keys and the values. */
	    job.setOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    
	    /** Generate our input, 5 files full of randomly ordered data.
	     *  Each line will start with either <code>Reduce 1</code> or <code>Reduce 2</code>
	     *  and then have a sequence number followed by a tab.
	     *  <ul> 
	     *  <li>Key <code>Reduce [01] ItemNo</code></li>
	     *  <li>Value Item <code>ItemNo</code> .... <code>line number</code>
	     *  </ul>
	     *  The start value is biased before and after by 1/2 of the count
	     */
	    //                   File Name                     , Key Leader, start,           count for item numbers
	    generateDataFile(fs, new Path(inputPath, "Piece 1"), "Reduce 1", start,           count);
	    generateDataFile(fs, new Path(inputPath, "Piece 2"), "Reduce 1", start - count/2, count );
	    generateDataFile(fs, new Path(inputPath, "Piece 2"), "Reduce 1", start + count/2, count );
	    generateDataFile(fs, new Path(inputPath, "Piece 3"), "Reduce 2", start,           count);
	    generateDataFile(fs, new Path(inputPath, "Piece 4"), "Reduce 2", start - count/2, count );
	    generateDataFile(fs, new Path(inputPath, "Piece 5"), "Reduce 2", start + count/2, count );
	    
	    
	    /** We have set our partitioner to key on the number after Reduce in the following data files, 
	     * and we are only using 1 and 2, so only require 2 reduces.
	     */
	    job.setNumReduceTasks(2);
	    
	    /** Set a partitioner that uses the second word in the key, numerically */
	    job.set("map.output.key.field.separator", " "); /** Use space as a field separator. */
	    
	    /** This also sets the PartitionerClass to KeyFieldBasedPartitioner */
	    job.setKeyFieldPartitionerOptions("-k 2,2"); /** Only use field 2, and nothing more for the partitioning. */
	    
	    /** Set up our grouping operator. 
	     * We have not changed the outputComparator as there is no need in this case.
	     *  We want full lexical sort on the keys.
	     */
	    job.setOutputValueGroupingComparator(ValueGroupingComparator.class);
	    
	    /** Send the job to the framework. */
	    launch(job);
	    
	    /** Collect the files out of the results directory and list them one by one */
	    FileStatus[] statai = fs.listStatus(outputPath);
	    /** This call can return null if the directory is empty. */
	    if (statai==null) {
	    	throw new IOException("no output found in " + outputPath);
	    }
	    /* The key and value objects will be reused on each read. */
	    Text key = new Text();
	    Text value = new Text();
	    /** For each entry in the directory, lets take a peek. */
	    for( FileStatus status : statai) {
	    	if (status.isDir()) { /** We only want to operate on our output files, there are other items in the directory. */
	    		continue;
	    	}
	    	LOG.info( "Reduce part " + status.getPath());
	    	final FileSplit split = new FileSplit( status.getPath(), 0, status.getLen(), (String[])null);
	    	RecordReader<Text,Text> reader = null;
	    	try {
	    		/** Set up a reader for the sequence file. The key/value types are actually recorded in the sequence file. */
				reader = new SequenceFileRecordReader<Text,Text>( job, split );
				if (reader==null) {
					LOG.warn( "Can not open " + status.getPath() + " skipping");
				}
				while (reader.next(key, value)) { /** Read and output each key, value pair in the sequence file.*/
					LOG.info( key + ", " + value);
				}
	    	} catch( IOException e ) {
	    		LOG.error( "Skipping file " + status.getPath() + " probably not a sequence file", e);
			} finally { /** Always ensure files are closed. */
				if (reader!=null) {
					reader.close();
				}
			}
	    }
	    return 0;
	  }
	  
	  /** This is a stylized method that the Tool pattern requires.
	   * @param job The {@link JobConf} object to use to launch the job.
	   */
	protected void launch(JobConf job) throws IOException {
		JobConf conf = new JobConf(job, this.getClass());
		
		RunningJob rj = JobClient.runJob(conf);
		if (!rj.isSuccessful()) {
			throw new IOException( "Job Failed");
		}
		
	}

	/**
	 * It works in conjunction with 
	 * {@link GenericOptionsParser} to parse the 
	 * <a href="{@docRoot}/org/apache/hadoop/util/GenericOptionsParser.html#GenericOptions">
	 * @param args The standard hadoop arguments startInt countInt
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new OutputComparatorAndGrouping(), args);
		System.exit(res);
	}

	/** Write some serialized data to a file containing structured key and value data, to demonstrate output value grouping.
	 * 
	 * The data written out is "primaryKey COUNTTABItem COUNT from FILE for START -> end ordinal EOL
	 * @param fs The file system to create the file in
	 * @param file The file name to create
	 * @param primaryKey The prefix value for the primary key - the portion of the key used by the partitioner.
	 * @param start The starting ordinal number for the records
	 * @param count The number of records to write.
	 * @throws IOException
	 */
	public static void generateDataFile( final FileSystem fs, final Path file, final String primaryKey, final int start, final int count) throws IOException
	{
		/** Simple sanity check */
		if (start<0||count<=0) {
			throw new IOException( "Invalid start " + start + " or count " + count + " for " + file + ", " + primaryKey);
		}
		
		/** Create the file and fail if the result is odd */
		FSDataOutputStream out = null;
		
		try {
			out = fs.create(file);
			if (out==null) {
				throw new IOException( "Unable to create output file " + file);
			}
			/** Create an output format object that will write to the newly created file */
			final Formatter fmt = new Formatter(out);
			/** Write the output data */
			final int end = start + count;
			/** Build an array of indices and randomize them to demonstrate reduce sorting */
			int[] indices = new int[count];
			/** Fill the indices array with ordered initial values */
			for( int i = 0; i < count; i++ ) {
				indices[i] = start + i;
			}
			final Random r = new Random();
			int tmpVal;
			int randomIndex;
			/** Like the (@link java.util.Collections#shuffle()) .*/
			for (int i= count; i>1; i--) {
				tmpVal = indices[i-1];
				randomIndex = r.nextInt(i);
				indices[i-1] = indices[randomIndex];
				indices[randomIndex] = tmpVal;
			}
			for ( int i = 0; i < count; i++ ) {
				fmt.format( "%s %d\tItem %d from %s for %d -> %d ordinal %d%n", primaryKey, indices[i], indices[i], file, start, end, i);
			}
			/** Force the output out to the descriptor in case there is some buffering */
			fmt.flush();
		} finally {
			/** It is especially important in Hadoop to ensure that any objects that hold file descriptors are closed.
			 * at least through Hadoop 0.19.0, sync/flush has no effect and blocks are written to HDFS ONLY when a full filesystem blocksize worth of data has been written or the file is closed.
			 * There are also numerous hard to diagnose failure cases from running out of file descriptors.
			 */
			if (out!=null) {
				out.close();
			}
		}
		
	}

}
