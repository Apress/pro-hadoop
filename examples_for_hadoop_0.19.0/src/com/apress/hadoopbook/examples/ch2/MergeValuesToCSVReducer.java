package com.apress.hadoopbook.examples.ch2;

import java.io.IOException;

import java.util.Iterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/** Merge the output values into a CSV format, pass all records through.
 *
 * This class is generic, it only specifies that the output value will be a Text Object.
 */
public class MergeValuesToCSVReducer<K, V>
    extends MapReduceBase implements Reducer<K, V, K, Text> {

	public static final String TOTAL_OUTPUT_RECORDS = "TOTAL_OUTPUT_RECORDS";
	public static final String TOTAL_VALUES = "TOTAL_VALUES";
	private static Logger logger = Logger.getLogger(MergeValuesToCSVReducer.class);
	/** Used to construct the merged value in. The {@link Text#set(String) Text.set} method is used to prevent object churn. */
	protected Text mergedValue = new Text();
	/** Working storage for constructing the resulting string. */
	protected StringBuilder buffer = new StringBuilder();
	
	public static final String OUTPUT = "Output";
	public static final String TOTAL_KEYS = "total keys";
	public static final String MAX_VALUES = "maximum value count";
	public static final String TOTAL_RECORDS = "total records";
    public static final String EXCEPTIONS = "Exceptions";
	public static final String EXCEPTION = "Exception";
	
	/** The maximum count of values seen for a key. */
	int maxValueCount = 0;
	/** Saved reporter object to enable reporting in the close method. */
	protected Reporter reporter;

	/** Keep track of the maximum number of keys a value had.
	 * Report it in the counters so that per task counters can be examined as needed
	 * and set the task status to include this maximum count.
	 */
	@Override
	public void close() throws IOException {
		super.close();
		if (reporter!=null) {
			reporter.incrCounter( OUTPUT, MAX_VALUES, maxValueCount );
			reporter.setStatus( "Job Complete, maxixmum ValueCount was " + maxValueCount );
		}
	}

	/** Merge the values for each key into a CSV text string.
	 * 
	 * @param key The key object for this group.
	 * @param values Iterator to the set of values that share the <code>key</code>.
	 * @param output The {@link OutputCollector} to pass the transformed output to.
	 * @param reporter The reporter object to update counters and set task status.
	 * @exception IOException if there is an error.
	 */
	public void reduce(K key, Iterator<V> values,
					   OutputCollector<K, Text> output, Reporter reporter)
		throws IOException {
		try {
			this.reporter = reporter;
			reporter.incrCounter( OUTPUT, TOTAL_KEYS, 1 );
			buffer.setLength(0);
			int valueCount = 0;
			for (;values.hasNext(); valueCount++) {
				reporter.incrCounter( OUTPUT, MergeValuesToCSVReducer.TOTAL_VALUES, 1 );
				String value = values.next().toString();
				if (value.contains("\"")) { // Perform excel style quoting
					value.replaceAll( "\"", "\\\"" );
				}
				buffer.append( '"' );
				buffer.append( value );
				buffer.append( "\"," );
			}
			buffer.setLength( buffer.length() - 1 );
			mergedValue.set(buffer.toString());
			reporter.incrCounter( OUTPUT, MergeValuesToCSVReducer.TOTAL_OUTPUT_RECORDS, 1 );
			output.collect( key, mergedValue );
			if (valueCount>maxValueCount) {
				maxValueCount = valueCount;
			}
		} catch( Throwable e ) {
			reporter.incrCounter( OUTPUT, EXCEPTION, 1 );
			reporter.incrCounter( EXCEPTIONS, e.getClass().getName(), 1 );
			logger.error( "Failed to process key " + key, e );
		}
	}
	
}
