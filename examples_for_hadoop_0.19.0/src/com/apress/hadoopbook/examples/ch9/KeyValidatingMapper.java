/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;

/** Perform simple validation on the keys.
 * 
 * The key helper defaults to {@link TextKeyHelperWithSeparators} and the configuration key <code>range.key.helper</code> is used
 * to lookup the key helper.
 * 
 * @author Jason
 *
 */
 class KeyValidatingMapper extends ExamplesMapReduceBase
					implements Mapper<Text, Text, Text, Text> {
	
	 public static Logger LOG = Logger.getLogger(KeyValidatingMapper.class);
	/** The key helper, this could be shared by all maps in the chain if we put them all in the same file
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


	/** validate the key values, as being >= 0 and < 2^32 and if needed swap the begin and end range for search requests.
	 * 
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		try {
			if (!helper.getFromRaw(key)) {
				reporter.incrCounter("KeyvalidatingMapper", "INVALID KEYS", 1);
				return;
			}
			if (helper.isSearchRequest()) {
				reporter.incrCounter("KeyValidatingMapper", "TOTAL SEARCH", 1);

				if (helper.getSearchRequest()<0 || helper.getSearchRequest()>4294967296L) {
					if (LOG.isDebugEnabled()) { LOG.debug("Search Key out of range [" + key + "]"); }
					reporter.incrCounter("KeyValidatingMapper", "SEARCH OUT OF RANGE", 1);
					return;
				}
				output.collect( key, value);
				return;

			} else {
				reporter.incrCounter("KeyValidatingMapper", "TOTAL SPACE", 1);

				if (helper.getBeginRange()<0||helper.getBeginRange()>4294967296L) {
					reporter.incrCounter("KeyValidatingMapper", "SPACE BEGIN OUT OF RANGE", 1);
					return;
				}
				if (helper.getEndRange()<0||helper.getEndRange()>4294967296L) {
					reporter.incrCounter("KeyValidatingMapper", "SPACE END OUT OF RANGE", 1);
					return;
				}

				/** Verify the ordering of the search space item. */
				if (helper.getBeginRange()<=helper.getEndRange()) {
					output.collect(key, value);
					return;

				} else {
					reporter.incrCounter("KeyValidatingMapper", "SPACE OUT OF ORDER", 1);
					long tmp = helper.getBeginRange();
					helper.setBeginRange(helper.getEndRange());
					helper.setEndRange(tmp);
					helper.setToRaw(outputKey);
					output.collect( outputKey, value);
					return;
				}
			}
		} catch( Throwable e ) {
			throwsIOExcepction(reporter, "KeyValidatingMapper", e);
		}
		 
		
	}

}
