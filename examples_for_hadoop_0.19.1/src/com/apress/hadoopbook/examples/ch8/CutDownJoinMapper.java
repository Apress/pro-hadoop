package com.apress.hadoopbook.examples.ch8;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;

/** A cut down join mapper that does very little but demonstrates using the TupleWritable
 * 
 * @author Jason
 *
 */
class CutDownJoinMapper extends MapReduceBase implements Mapper<Text,TupleWritable,Text,Text> {

	Text outputValue = new Text();

	@Override
	public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			
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
					output.collect( key, new Text( value.get(i).toString() ) );
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
