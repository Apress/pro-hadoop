/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/** This class will partition the full IPV4 range into spans so that the number of spans is the number of partitions N
 * and each span covers roughly 1/Nth of the IPV4 address space, and the spans are contiguous.
 * As a partitioner it accepts search request and search space keys, the values are ignored.
 * The search request key determines the span, the search space key {@link PartitionedTextKeyHelperWithSeparators#getBeginRange()} determines the span
 * In theory any search space key making it to the partitioner has a virtual span that fits the partition span. 
 * 
 * 
  * As a thought at reducing complexity, a TreeMap was used to provided an ordered set, that operations like tailSet could be used on
 * In retrospect this was an error as the number of range elements is small enough that simple brute force calculations should be
 * faster than the magic of the collection classes, and churn far fewer objects.
 * 
 * The original plan was simply to throw all of the incoming log lines, in with the range set and let the map process handle spanning the keys
 * by calling spanSpaceKeys for any range key that it received.
 * 
 * There were two insights after the fact, 1) the key space is small enough that just ordering it and keeping N copies where N is the number of reduces is viable
 * This opens the door to using the mapside join which is perhaps the very fastest way to do this.
 * 
 * The other is that the key space could be prebuild, spanned for the space, and then there is no need for the mapper to be aware of spanSpaceKeys
 * 
 * The concept is that each partition has addresses from 1 segment of the whole IP address space, if there are 2 partitions, one covers 0.0.0.0 to
 * 127.255.255.255 and partition 2 gets 128.0.0.0 to 255.255.255.255
 * any log address between 0.0.0.0 and 127.255.255.255 goes to partition 0 and any ip address between 128.0.0.0 and 255.255.255.255 goes to partition 1.
 * 
 * A search space key that was original say 127.0.0.0:129.0.0.0 SuperNet, would be output as:
 * <ul>
 * <li>127.0.0.0:127.255.255.255:127.0.0.0:129.0.0.0\tSuperNet</li>
 * <li>128.0.0.0:129.0.0.0:127.0.0.0:129.0.0.0\tSuperNet</li>
 * </ul>
 * 

 * This class also acts as a {@link Partitioner} for range span keys and incoming ip address keys.
 * 
 * @author Jason
 *
 */
public class SimpleIPRangePartitioner   implements Partitioner<Text, Text> {
	/** Our logger. */
	static Logger LOG = Logger.getLogger(SimpleIPRangePartitioner.class);

	JobConf conf;
	PartitionedTextKeyHelperWithSeparators helper;
	/** There are at most a few hundred ranges here, they could be easily handled. In the interests of minimizing code complexity
	 * a collection is used, with all of the object churn overhead that entails.
	 */
	TreeMap<Long,Integer> ranges;
	@Override
	public int getPartition(final Text key, final Text value, final int numPartitions) {
		if (!(helper.getFromRaw(key) && helper.isValid())) {
			throw new IllegalArgumentException("key " + key + " can not be parsed as a network range set");
		}
		/** The IP address that effectively defines this range. */
		final long begin;

		if (helper.isSearchRequest()) {
			begin = helper.getSearchRequest();
		} else {
			begin = helper.getBeginRange();
		}
				
		/** Find the bucket in ranges that is the lowest bucket that is valued higher than begin.
		 * that bucket's partition is the partition for this value.
		 */
		final Entry<Long, Integer> partition = ranges.ceilingEntry(Long.valueOf(begin));
		/** Stored as a variable for debugging ease */
		final int realPartition = partition.getValue();
		
		assert (helper.isSearchSpace() ? partition.getKey() >= helper.getEndRange() : true) : String.format( "search space range end %08x exceeds partition limit %0x8", helper.getEndRange(), partition.getKey());
		
		return realPartition;

	}

	/** This method builds the span table based on the number of paritiones extracted from the configuration via {@link JobConf#getNumReduceTasks()}
	 * 
	 * @see Partitioner#configure(JobConf)
	 */
	@Override
	public void configure(JobConf job) {
		conf = job;
		/** Now that we have a conf object we can initialize the helper and build ranges, using the number of reduces. */
		helper = new PartitionedTextKeyHelperWithSeparators(conf);
		
		final int numPartitions = conf.getNumReduceTasks();
		
		ranges = new TreeMap<Long,Integer>();
		
		long rangeSpan = 4294967296L / numPartitions;
		
		/** The partition that ends at <code>spanned</code> */
		int partition = 0;
		/** The end of the address space already in ranges. */
		long spanned;
		/** The value stored is the end of the range, the range starts at the previous value + 0, or for the first value
		 * at 0.
		 * Note that the test is less than and not less than or equals, the ranges have to end at 2^32-1 as we only have 32 bits.
		 */
		for (spanned= rangeSpan; spanned < 4294967296L; spanned += rangeSpan, partition++) {
			ranges.put( spanned, partition );
		}
		/** first address is 0, last address is 2^32 - 1, make sure we cover all the way to the end of the range if
		 * the 2^32/numPartitions is not an integer. The last partition may be a small */
		if (spanned>4294967296L-1){
			ranges.put( 4294967296L -1, partition);	/** The end range */
		}
	}
	
	/** This method is the work house used to generate spanning keys for a search space item. *
	 * 
	 * For each input key, which is taken from <code>outsideHelper</code> a spanning key will be generated for each partition
	 * that overlaps with the range of the key.
	 * 
	 * @param outsideHelper The {@link PartitionedTextKeyHelperWithSeparators} that contains the parsed key object}
	 * @param forConstructedKeys {@link Text} object to use for constructing the spanning keys.
	 * @param value {@link Text} object to output with each spanning key.
	 * @param output {@link OutputCollector} to use for writing the spanning keys to.
	 * @param reporter {@link Reporter} object to use for counters and errors.
	 * @return the number of keys output.
	 * @throws IOException
	 */
	public int spanSpaceKeys( PartitionedTextKeyHelperWithSeparators outsideHelper, 
			Text forConstructedKeys, final Text value, 
			final OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		/** If the key isn't valid bail. */
		if (!outsideHelper.isValid()) {
			reporter.incrCounter("KeySpanning", "Invalid Keys", 1);
			throw new IllegalArgumentException("Can not span invalid keys");
		}
		
		/** This could just pass the key forward quietly. */
		if (!outsideHelper.isSearchSpace()) {
			reporter.incrCounter("KeySpanning", "Not Search Space", 1);
			throw new IllegalArgumentException("Can not span search request keys");
		}
		
		/** If the passed in key is a regular search space key, set the extended attributes for a spanning search space key. */
		if (!outsideHelper.isHasRealRange()) {
			outsideHelper.setRealRangeBegin(outsideHelper.getBeginRange());
			outsideHelper.setRealRangeEnd(outsideHelper.getEndRange());
		}
		
		/** The {@link #ranges} keys are the end of the range for that partition, the tail set
		 * from {@link #ranges} for the actual beginning of the range for <code>outsiteHelper</code> starts
		 * with the first partition that could contain keys that could match the the key in <code>outsideHelper</code>. 
		 * 
		 * This is the partition that has a span end = or > our key's beginning.
		 * Any span before this, would only contain keys that would never match this search space request
		 * so there is no need to write a spanning key to them.
		 */
				
		NavigableMap<Long, Integer> spannedRanges = ranges.tailMap(outsideHelper.getRealRangeBegin(), true);
		
		/** The loop below uses the the begin range of <code>outsideHelper</code> as the start point for the next
		 * output record.
		 * the end range value is used as a convenience and should not be used in test.
		 * The real end and real beginning are always the actual being and end of the search space request.
		 */
		helper.setBeginRange( outsideHelper.getRealRangeBegin());
		helper.setRealRangeBegin( outsideHelper.getRealRangeBegin());
		helper.setEndRange( outsideHelper.getRealRangeEnd());
		helper.setRealRangeEnd( outsideHelper.getRealRangeEnd());

		int count = 0;
		/** The real ranges are untouched, and the beginrange is moved up and the endrange is just set in the loop.
		 * When end range <= the spanEnd no more ranges are spanned.
		 * the value of getEndRange() is never valid for use in tests.
		 */
		if (LOG.isDebugEnabled()) { LOG.debug(String.format("Spanning key %x:%x %s", helper.getRealRangeBegin(), helper.getRealRangeEnd(),value)); }
		for( Map.Entry<Long, Integer> span : spannedRanges.entrySet()) {
			final Long spanEnd = span.getKey();

			/** If the newly adjusted beginRange is past the end of our key's range, there will be no more keys output. so finish up */
			if (helper.getBeginRange()>helper.getRealRangeEnd()) {
				helper.isValid = false;
				break; /** Done no more ranges spanned. We could just return count from here, but this way there is only one valid exit point */
			}


			/** This should never happen. */
			if (spanEnd.longValue() < helper.getBeginRange()) {	/** at least a partial span. */
				throw new IOException( String.format("Constraint failure, the partition end %d %x is less than the key begin %d %x", spanEnd, spanEnd, helper.getBeginRange(), helper.getBeginRange()) );
			}

 			/** The begin value for the current portion of <code>outsideHelper</code> is inside the
			 * span of this partition. We have to assume at this point that it is not before the start of the partition.
			 * 
			 * If the spanEnd >= the getRealRangeEnd, this output key is contained entirely within this partition
			 * 
			 */

				
			/** This case indicates that the end of this partition span is past the end of the real search space request.
			 * This is the last key that will be output, the output key end to be the real end and finish
			 */
			if (spanEnd.longValue()>=helper.getRealRangeEnd()) { /** The range of the key only extends to this partition. */
				helper.setEndRange(helper.getRealRangeEnd());
				if (LOG.isDebugEnabled()) { LOG.debug(String.format(">= spanEnd %x %x of %x:%x %s", spanEnd, helper.getRealRangeEnd(), helper.getRealRangeBegin(), helper.getRealRangeEnd(), value ) ); }

			} else {	// There will be at least one more output key after this one

				/** In this case, the search space real end is past the end of this partition, output a record from the
				 * begin that was setup on the previous run through here or the initial condition and an end == to the span end
				 * and continue our loop
				 */

				helper.setEndRange(spanEnd.longValue()); // Has to be less than the real end range
				if (LOG.isDebugEnabled()) { LOG.debug(String.format(" < spanEnd  %x %x:%x  %x:%x %s", spanEnd, helper.getBeginRange(), helper.getEndRange(), helper.getRealRangeBegin(), helper.getRealRangeEnd(), value ) ); }


			}

			count++;
			helper.setToRaw(forConstructedKeys);
			output.collect(forConstructedKeys,value);
			helper.setBeginRange(helper.getEndRange()+1); // One past the last record output
			reporter.incrCounter("KeySpanning", "Partition " + span.getValue(), 1);

		}
		reporter.incrCounter("KeySpanning", "OUTPUT KEYS", count);
		return count;
	}
}
