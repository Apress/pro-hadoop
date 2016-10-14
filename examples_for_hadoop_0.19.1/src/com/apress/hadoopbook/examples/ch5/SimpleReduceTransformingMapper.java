package com.apress.hadoopbook.examples.ch5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/** Transform the input textual values into LongWritables
 * 
 * @author Jason
 *
 */
public class SimpleReduceTransformingMapper extends MapReduceBase implements
		Mapper<Text, Text, LongWritable, LongWritable> {
	/** Recycle these objects to save on object generation. */
	LongWritable outputKey = new LongWritable();
	LongWritable outputValue = new LongWritable();
	
	/** Simple mapper that just transforms the key and value into {@link LongWritable}.
	 * 
	 */
	@Override
	public void map(Text key, Text value,
			OutputCollector<LongWritable, LongWritable> output,
			Reporter reporter) throws IOException {
		reporter.incrCounter("Map Input Keys", "Total", 1);
		try {
			/** Convert the input into longs. */
			outputKey.set( Long.valueOf(key.toString()));
			outputValue.set( Long.valueOf(value.toString()));
			
			/** send the output to the reduce. */
			output.collect(outputKey, outputValue);
			reporter.incrCounter("Map Output Keys", "Total", 1);
		} catch (Throwable e) {
			/** report on a failure. */
			reporter.incrCounter("Map Input Keys", "Failures", 1);
			if (e instanceof IOException) {
				throw (IOException) e;
			}
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			throw new IOException(e);
		}
		
		
	}
	

}