package com.apress.hadoopbook.examples.ch8;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

/** A cut down join mapper that does very little but demonstrates using the TupleWritable
 * 
 * @author Jason
 *
 */
class RePartitioningoinMapper extends MapReduceBase implements Mapper<Text,TupleWritable,Text,Text> {
	public static Logger LOG = Logger.getLogger(RePartitioningoinMapper.class);
	@Override
	public void close() throws IOException {
		super.close();
		
		if(savedKey==null) {
			LOG.warn("No saved key for map task");
			return;
		}
		if (realOutput==null) {
			LOG.warn("No real Output object for map task");
			return;
		}
		/** The close will throw an exception if no records have been written. */
		realOutput.close(Reporter.NULL);
		try {
		fs.rename(realOutputPath, 
				new Path( realOutputPath.getParent(), 
						String.format("joinpart-%05d", computePartition(conf, savedKey, savedValue))));
		} catch( Exception e) {
			throw new IOException( String.format("Unable to rename %s to partition based name"), e);
			
		}
	}

	/** Compute a partition given a key and a value, for a map side join key.
	 * @param conf The job conf object in effect. Used to get the partitioner object, {@link HashPartitioner} used if no partitioner defined.
	 * @param key The key to base the partition on.
	 * @param value The value to use, null is commonly acceptable unless the partitioner is unusual
	 * @return the partition number for this key, and actually for this input slice.
	 * @throws InstantiationException If the partitioner can't be instantiated
	 * @throws IllegalAccessException If the partitioner constructor is not public.
	 */
	@SuppressWarnings("unchecked")
	public static int computePartition( JobConf conf, Text key, Text value) throws InstantiationException, IllegalAccessException {
		Partitioner<Text,Text> partitioner = null;
        if ((partitioner = conf.getPartitionerClass().newInstance())==null) {
            LOG.info( "No partitioner class defined" );
            /** Note: these are the output key, value types */
            partitioner = new HashPartitioner<Text,Text>();
        }
        return partitioner.getPartition( key, value, conf.getNumMapTasks());

	}
	JobConf conf;

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		conf = job;
		
		/** for hadoop 19, */
		/** We have to rely on the TextOutputFormat using the same root as the method used to get the job output path */
		//realOutputPath = FileOutputFormat.getTaskOutputPath(job, UUID.randomUUID().toString());
		/** For hadoop 18 or less */
		realOutputPath = new Path( FileOutputFormat.getOutputPath(conf), UUID.randomUUID().toString());
	
		
		try {
			 fs = realOutputPath.getFileSystem(conf);

			 
			 /** Choose only one of these. */
			 realOutput = getSequenceFileOutput(fs, realOutputPath);
			 realOutput = getTextFileForOutput(fs, realOutputPath);
			 
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	protected RecordWriter<Text,Text> getSequenceFileOutput(final FileSystem fs, Path path)
			throws IOException {
		/** For a Sequence File, only pick 1 */
		 CompressionCodec codec = null;
		 CompressionType compressionType = CompressionType.NONE;
		 if (SequenceFileOutputFormat.getCompressOutput(conf)) {
			 // find the kind of compression to do
			 compressionType =  
				 SequenceFileOutputFormat.getOutputCompressionType(conf);

			 // find the right codec
			 ReflectionUtils.newInstance(SequenceFileOutputFormat.getOutputCompressorClass(conf, DefaultCodec.class), conf);
		 }
		 final SequenceFile.Writer out = 
			 SequenceFile.createWriter(fs, conf, path,
					 Text.class,
					 Text.class,
					 compressionType,
					 codec,
					 Reporter.NULL);

		 return new RecordWriter<Text,Text>() {

			 public void write(Text key, Text value)
			 throws IOException {

				 out.append(key, value);
			 }
			 public void close(Reporter reporter) throws IOException { out.close();}
		 };
	}

	protected RecordWriter<Text,Text> getTextFileForOutput(FileSystem fs, Path path) throws IOException {

		 
		/** For a Text Output File, only pick 1 */

		 TextOutputFormat<Text,Text> textOutputFormat = new TextOutputFormat<Text,Text>();
		 return textOutputFormat.getRecordWriter(fs, conf, path.getName(), Reporter.NULL);
	}

	Text outputValue = new Text();
	Text savedKey = null;
	Text savedValue = null;
	/** The file system of realOutput, and realOutputPath, set in configure, and used in close.
	 * 
	 */
	FileSystem fs = null;
	/** This is where the real job output goes. not to output.collect. */
	RecordWriter<Text,Text> realOutput;
	
	/** The path to the file for the actual output.
	 * 
	 */
	Path realOutputPath;

	@Override
	public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			/** This is ugly and until 0.20 has to be done in this loop. */
			if (savedKey==null) {
				savedKey = key;
			}
			/** The user has two choices here, there is an iterator and a get(i) size option.
			 * The down side of the iterator is you don't know what table the value item comes from.
			 */
			
			/** Gratuitous demonstration of using the TupleWritable iterator. */
			int valueCountTotal = 0;
			for( @SuppressWarnings("unused") Writable item : value) {
				valueCountTotal++;
			}
			reporter.incrCounter("Map Value Count Histogram", key.toString() + " " + valueCountTotal, 1);


			/** Act like the Identity Mapper. */
			final int max = value.size();
			int valuesOutputCount = 0;
			for( int i = 0; i < max; i++) {
				if (value.has(i)) {	// Note, get returns the same object initialized to the data for the current get
					realOutput.write( key, new Text( value.get(i).toString() ) );
					valuesOutputCount++;
				}
			}
			assert valueCountTotal == valuesOutputCount : "The interator must always return the same number of values as a loop monitoring has(i)";
		} catch (Throwable e) {
			reporter.incrCounter("Exceptions", "MapExceptionsTotal", 1);
			MapSideJoinExample.LOG.error( "Failed to handle record for " + key, e);
		}
	}
}
