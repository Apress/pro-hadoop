package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;

/** A mapper that handles producing a set of keys that span all of the partitions that the range of an incoming search space key covers. */
public class RangePartitionTransformingMapper extends ExamplesMapReduceBase implements Mapper<Text,Text,Text,Text>{
	/** The key parsing helper for handling keys that may have a span range and a real range. */
	PartitionedTextKeyHelperWithSeparators helper;
	/** New keys are generated and this object is used for their construction. */
	Text outputKey = new Text();
	/** The partitioner actually has the {@link SimpleIPRangePartitioner#spanSpaceKeys(PartitionedTextKeyHelperWithSeparators, Text, Text, OutputCollector, Reporter)} method.
	 * 
	 */
	SimpleIPRangePartitioner partitioner;
	
	/** For each incoming key that is a search space key, output N key each output key covers the area that the input key covers
	 * in the range of the each partition.
	 * 
	 *  he concept is that each partition has addresses from 1 segment of the whole IP address space, if there are 2 partitions, one covers 0.0.0.0 to
	 * 127.255.255.255 and partition 2 gets 128.0.0.0 to 255.255.255.255
	 * any log address between 0.0.0.0 and 127.255.255.255 goes to partition 0 and any ip address between 128.0.0.0 and 255.255.255.255 goes to partition 1.
	 * 
	 * A search space key that was original say 127.0.0.0:129.0.0.0 SuperNet, would be output as:
	 * <ul>
	 * <li>127.0.0.0:127.255.255.255:127.0.0.0:129.0.0.0\tSuperNet</li>
	 * <li>128.0.0.0:129.0.0.0:127.0.0.0:129.0.0.0\tSuperNet</li>
	 * </ul>
	 */
	@Override
	public void map(Text key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			reporter.incrCounter("RangePartitionTransformingMapper", "INPUT KEYS", 1);
			if (!helper.getFromRaw(key)) {
				reporter.incrCounter("RangePartitionTransformingMapper", "Invalid Keys", 1);
				return;
			}
			if (helper.isSearchRequest()) {
				output.collect(key, value);
				reporter.incrCounter("RangePartitionTransformingMapper", "Request Keys", 1);
				return;
			}
			partitioner.spanSpaceKeys(helper, outputKey, value, output, reporter);
		} catch( Throwable e) {
			throwsIOExcepction( reporter, "RangePartitionTransformingMapper", e);
		}
		
		
	}

	/** Set up our local objects and fire the base class 
	 * @see com.apress.hadoopbook.utils.ExamplesMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	public void configure(JobConf job) {
		super.configure(job);
		helper = new PartitionedTextKeyHelperWithSeparators(conf);
		partitioner = new SimpleIPRangePartitioner();
		partitioner.configure(conf);
	}

}
