package com.apress.hadoopbook.examples.ch8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;

/** Produce a single output record for each tuple, with an ascii tab separating each potential value.
 * 
 * If a particular table doesn't have a value for this key, an empty string will be output.
 * An ascii tab separates each table value.
 * If table 1 has 'a', table 2 has 'b' and table 3 has no value,
 * an output value of the form <code>a\tb\t</code> will be written.
 * 
 * If table 1 has 'a', table 2 has 'b' and table 3 has c,
 * an output value of the form <code>a\tb\tc</code> will be written.
 * 
 * @author Jason
 *
 */
class DuplicateKeyIndicatingIdentityMapper extends MapReduceBase implements Mapper<Text,TupleWritable,Text,Text> {

	Text outputValue = new Text();
	StringBuilder sb = new StringBuilder();
	Formatter fmt = new Formatter(sb);
	Text lastKey = new Text();
	ArrayList<String> accumulatedValuesForLastKey = new ArrayList<String>();
	OutputCollector<Text, Text> savedOutput;
	Reporter savedReporter;
	
	@Override
	public void close()
	{
		if(accumulatedValuesForLastKey.isEmpty()) {
			return;	/** No work to do, probably there were no keys ever, in the input. */
		}
		try {
			performReduce( null, savedOutput, null);
		} catch (IOException e) {
			MapSideJoinExample.LOG.error("Close failed ", e);
			savedReporter.incrCounter("Exceptions", "MapExceptionsTotal", 1);
		}
	}
	
	@Override
	public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			transFormTupleToOutputString(key, value, reporter);
			String transformedValue = sb.toString();

			if (accumulatedValuesForLastKey.isEmpty()) { // Start condition
				lastKey.set(key);
				savedOutput = output;	/** Somewhere past 0.20 this won't be necessary to make them available in the close. */
				savedReporter = reporter;
				outputValue.set("File A	A Line	A Repeat#	A Repeat Count	Sequence	File B	B Line	B Repeat #	B Repeat Count	Sequence	File C	Line C	Repeat # C	Repeat Count	Sequence	Key Repeat Count");
				/** Output some headers. */
				output.collect( new Text("Key"), outputValue );
			} else 	if (lastKey.equals(key)) {	/** Same key as last time, just accumulate the value. */
				accumulatedValuesForLastKey.add(transformedValue);
				return;
			}
			/** When we get here, key is different from old key, and old key's values are complete and need to be emitted. */
			performReduce(key, output, transformedValue);


		} catch (Throwable e) {
			reporter.incrCounter("Exceptions", "MapExceptionsTotal", 1);
			MapSideJoinExample.LOG.error( "Failed to handle record for " + key, e);
		}
	}

	/**
	 * @param key
	 * @param output
	 * @param transformedValue
	 * @throws IOException
	 */
	private void performReduce(Text key, OutputCollector<Text, Text> output,
			String transformedValue) throws IOException {
		try {
			/** This would normally be in the reduce, but because of the way map side joins work we can reduce here.*/

			/** output our data. */
			if (accumulatedValuesForLastKey.size()==1) {
				/** If only one value short circuit and pass the key and value on. */
				outputValue.set( accumulatedValuesForLastKey.get(0));
				output.collect( lastKey, outputValue );
			} else {

				/** Emit an output record for each value, added [X of Y] to the text of the value. X being valueCount, Y being valueTotalCount. */
				int valueCount = 1;
				for( String found : accumulatedValuesForLastKey ) {
					sb.setLength(0);
					fmt.format( "%s\t[%d of %d]", found, valueCount++, accumulatedValuesForLastKey.size() );
					fmt.flush();
					outputValue.set(sb.toString());
					output.collect(lastKey, outputValue);
				}
			}
		} finally {
			if(key!=null) {	/** are we being called from the {@link #close()} method? */
				lastKey.set(key);
				accumulatedValuesForLastKey.clear();
				accumulatedValuesForLastKey.add(transformedValue);
			}
		}
	}

	/** Produce a tab separated output record for the values contained in <code>value</code>
	 * 
	 * 
	 * 
	 * There will be one tab separated output group for each data set in the join.
	 * Place holder strings are emitted for values that are not present in individual data sets for tuple.
	 * The place holder strings have as many tabs as the first present value in the tuple.
	 * 
	 * @param key The record key, only used for logging
	 * @param value The {@link TupleWritable}
	 * @param reporter Used for reporting counters for histograms.
	 */
	private void transFormTupleToOutputString(Text key, TupleWritable value,
			Reporter reporter) {
		/** The user has two choices here, there is an iterator and a get(i) size option.
		 * The down side of the iterator is you don't know what table the value item comes from.
		 */
		
		/** Gratuitous demonstration of using the TupleWritable iterator. */
		sb.setLength(0);
		int valueCountTotal = 0;
		String emptyRecordString = null;	/** The output records are setup for import into excel, empty groups need to have the correct tab count emitted*/
		for( Writable item : value) {
			final String itemString = item.toString();
			if (emptyRecordString==null) {
				emptyRecordString = extractTabs(itemString);
			}
			fmt.format( "%s\t", itemString);
			valueCountTotal++;
		}
		fmt.flush();
		if (sb.length()>0) {
			sb.setLength(sb.length()-1);	/** Lose the trailing tab. */
		}
		MapSideJoinExample.LOG.info( "For key, the tuple values are: " + sb);
		/** Produce a counter set that is a histogram over the value counts. */
		sb.setLength(0);
		fmt.format( "ValueCounts %d", valueCountTotal); fmt.flush();
		reporter.incrCounter(sb.toString(), key.toString(), 1);
		
		/** Produce the output value record CSV tab separated line, */
		sb.setLength(0);
		final int max = value.size();
		for( int i = 0; i < max; i++) {
			String stringToOutput;
			if (value.has(i)) {
				stringToOutput = value.get(i).toString();
			} else {
				stringToOutput = emptyRecordString;
			}
			fmt.format( "%s\t", stringToOutput);
		}
		if (sb.length()>0) {
			sb.setLength(sb.length()-1);	/** Lose the trailing tab. */
		}
	}

	/** Extract a string that has as many tab characters as <code>item</code> does.
	 * This is used for outputting place holders for strings that are tab separated CSV records.
	 * @param item The string to scan for tab characters
	 * @return Just the tabs, maam.
	 */
	static public String extractTabs(CharSequence item) {
		final int itemLen = item.length();
		final StringBuilder sb = new StringBuilder(itemLen);
		
		
		for( int i = 0; i < itemLen; i++) {
			final char c = item.charAt(i);
			if (c=='\t') {
				sb.append(c);
			}
		}
		return sb.toString();
	}
}
