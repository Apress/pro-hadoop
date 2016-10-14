package com.apress.hadoopbook.examples.ch5;

import java.io.IOException;
import java.util.Formatter;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/** Demonstrate some aggregation in the reducer
 * 
 * Produce output records that are the key, the average, the count, the min, max and diff
 * 
 * @author Jason
 *
 */
public class SimpleReduceTransformingReducer extends MapReduceBase implements
		Reducer<LongWritable, LongWritable, Text, Text> {
	
	/** Save object churn. */
	Text outputKey = new Text();
	Text outputValue = new Text();
	
	/** Used in building the textual representation of the output key and values. */
	StringBuilder sb = new StringBuilder();
	Formatter fmt = new Formatter(sb);
	
	
	@Override
	public void reduce(LongWritable key, Iterator<LongWritable> values,
			OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		/** This is a bad practice, the transformation of the key should be done in the map. */
		reporter.incrCounter("Reduce Input Keys", "Total", 1);
		try {
			long total = 0;
			long count = 0;
			long min = Long.MAX_VALUE;
			long max = 0;
			
			/** Examine each of the values that grouped with this key. */
			while (values.hasNext()) {
				final long value = values.next().get();
				if (value>max) {
					max = value;
				}
				if (value<min) {
					min = value;
				}
				total += value;
				count++;
			}
			
			sb.setLength(0);
			fmt.format("%12d %3d %12d %12d %12d", total/count, count, min, max, max-min);
			fmt.flush();
			outputValue.set(sb.toString());
			
			sb.setLength(0);
			fmt.format("%4d", key.get());
			outputKey.set(sb.toString());
			
			reporter.incrCounter("Reduce Output Keys", "Total", 1);
			output.collect(outputKey, outputValue);
							
		} catch( Throwable e) {
			reporter.incrCounter("Reduce Input Keys", "Exception", 1);
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