package com.apress.hadoopbook.examples.ch8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/** This reducer simple collects all of the values associated with a key, and outputs N output records one for each value.
 * If there is more than one value for a key, the value output indicates which value it is in the set of values.
 * i.e.: if there are three values for a key, [1 of 3] will be appended to output for the first value [2 of 3] for the second and [3 of 3] for the third.
 * 
 * An ascii tab character separates the data in the values, to ease in loading the output into a spreadsheet.
 * 
 * @author Jason
 *
 */
class DuplicateKeyIndicatingIdentityReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
	/** Used for storing the constructed output values. */
	Text value = new Text();
	/** Used for building the output value. */
	StringBuilder sb = new StringBuilder();
	/** used fixed with numeric fields in the output. */
	Formatter fmt = new Formatter(sb);
	/** The set of values found for the key. */
	ArrayList<String> accumulatedValuesForKey = new ArrayList<String>();
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			accumulatedValuesForKey.clear();
			/** Collect all of the values in {@link #accumulatedValuesForKey}, and compute the total number of values. */
			int valueTotalCount = 0;
			while (values.hasNext()) {
				/** Note: each call to next will probably return the same value object. */
				accumulatedValuesForKey.add(values.next().toString());
				valueTotalCount++;
			}
			
			/** output our data. */
			if (accumulatedValuesForKey.size()==1) {
				/** If only one value short circuit and pass the key and value on. */
				value.set( accumulatedValuesForKey.get(0).toString());
				output.collect( key, value );
			} else {

				/** Emit an output record for each value, added [X of Y] to the text of the value. X being valueCount, Y being valueTotalCount. */
				int valueCount = 1;
				for( String found : accumulatedValuesForKey ) {
					sb.setLength(0);
					fmt.format( "%s\t[%d of %d]", found, valueCount++, valueTotalCount );
					fmt.flush();
					value.set(sb.toString());
					output.collect(key, value);
				}
			}
		} catch (Throwable e) {
			reporter.incrCounter("Exceptions", "MapExceptionsTotal", 1);
			MapSideJoinExample.LOG.error( "Failed to handle record for " + key, e);
		}
	}
}