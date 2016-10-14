/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;
import java.util.Formatter;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.apress.hadoopbook.examples.ch9.ActiveRanges.Range;
import com.apress.hadoopbook.utils.ExamplesMapReduceBase;

/** Simple reducer for merging keys with ranges.
 * 
 * The key helper defaults to {@link TextKeyHelperWithSeparators} and the configuration key <code>range.key.helper</code> is used
 * to lookup the key helper.
 * @author Jason
 *
 */
public class ReducerForStandardComparator extends ExamplesMapReduceBase implements Reducer<Text, Text, Text, Text> {

	/** The end helper, this could be shared by all maps in the chain if we put them all in the same file
	 * For clarity of code each is in a separate file. There are some interesting optimizations
	 * that could be done if they shared variables.
	 */
	TextKeyHelperWithSeparators helper;

	Text outputKey = new Text();
	Text outputValue = new Text();

	@Override
	public void configure(JobConf job){
		super.configure(job);
		helper =  ReflectionUtils.newInstance(conf.getClass("range.key.helper", TextKeyHelperWithSeparators.class,
                TextKeyHelperWithSeparators.class),conf); 
	}



	/** Keep track of the active ranges in a way that minimizes object churn. */
	ActiveRanges activeRanges = new ActiveRanges();


	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException {
		try {
			reporter.incrCounter("ReducerForStandardComparator", "TOTAL KEYS", 1);
			if (!helper.getFromRaw(key)) {
				reporter.incrCounter("ReducerForStandardComparator", "BAD KEYS", 1);
				return;
			}

			if (helper.isSearchSpace()) {
				reporter.incrCounter("ReducerForStandardComparator", "SPACE KEYS", 1);

				/** For simplicity, put all of the values in. */
				while (values.hasNext()) {
					final Text value = values.next();
					reporter.incrCounter("ReducerForStandardComparator", "SPACE VALUES", 1);
					activeRanges.activate( reporter, "ReducerForStandardComparator", helper, value.toString());
				}
				return;
			}

			if (helper.isSearchRequest()) {
				/** First, lets prune the activeRanges. */
				final long searchRequest = helper.getSearchRequest();
				activeRanges.deactivate(searchRequest);

				/** Because the ranges are removed when their end is less than end,
				 * and because keys are always sorted after the begin of a range
				 * all active ranges are now 'hits' for this search request.
				 */
				int max = activeRanges.size();
				while (values.hasNext()) {
					final Text value = values.next();
					for (int i = 0; i < max; i++) {
						ActiveRanges.Range<String> hit = activeRanges.get(i);
						handleHit( key, output, reporter, value, hit);
					}
				}


			}

		} catch( Throwable e ) {
			throwsIOExcepction(reporter, "ReducerForStandardComparator failed", e);
		}

	}

	StringBuilder sb = new StringBuilder();
	Formatter fmt = new Formatter(sb);
	protected void handleHit(Text key,
			OutputCollector<Text, Text> output, Reporter reporter, Text value, Range<String> hit) throws IOException {
		/** For this version we leave the end alone. */
		sb.setLength(0);
		fmt.format( "%s\t%s", hit.getValue(), value.toString());
		fmt.flush();
		outputValue.set(sb.toString());
		sb.setLength(0);
		fmt.format("%8.8s\t%08x\t%08x", key.toString(), hit.getBegin(), hit.getEnd()); fmt.flush();	/** Loose the suffix */
		outputKey.set(sb.toString());
		output.collect( outputKey, outputValue );


	}


}
