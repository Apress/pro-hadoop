package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;

/** Class to perform mergers of the output from the {@link BruteForceMapReduceDriver}, also a good example of how to merge
 * a set of reduce output partitions.
 * This instance only works if the mapred.join.keycomparator parameter is set to {@link IPv4TextComparator}.
 */
public class DataJoinMergeMapper extends ExamplesMapReduceBase implements
		Mapper<Text, TupleWritable, Text, Text> {


	protected static Logger LOG = Logger.getLogger(DataJoinMergeMapper.class);

	/** both {@link #outputText} and {@link #values} will pin objects, but it doesn't seem worth the bother
	 * to zero them at the end of the map call or in a close method.
	 */

	/** The constructed text for tuples that are not of type text. */
	Text []outputText;
	/** Initialized in map, the first time through to be large enough to hold a full row set.
	 * Each element will either be a value or a pointer to {@link #outputText} holding a converted value
	 * If multiple appear, they are sorted per {@link TabbedNetRangeComparator}.
	 */
		
	Text[] values;
	
	


	TabbedNetRangeComparator comparator = new TabbedNetRangeComparator();

	
	@Override
	public void map(Text key, TupleWritable value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		try {
			reporter.incrCounter("DataJoinReduceOutput", "Input Keys", 1);
			/** The number of tables in the join. */
			final int size = value.size();
			/** Allocate the values array if needed. a null indicates end, so one extra allocated */
			if (values==null) {
				values = new Text[size+1];
				outputText = new Text[size];
				/** Make some {@link Text} items, just in case. these probably arn't needed but are made only once. */
				for (int i = 0; i < size; i++ ) {
					outputText[i] = new Text();
				}
			}
			/** For each table, check to see if it has a value for the key.
			 * if it does, store it in values, possibly converting it to a text object by calling {@link Text#set(String)} with the
			 * with the string conversion.
			 */
			/** The current index to store into values. */
			int valuesIndex = 0;
			for (int i = 0; i < size; i++) {
				if (value.has(i)) {
					Writable outputValue = value.get(i);
					if (outputValue instanceof Text) {
						values[valuesIndex] = (Text) outputValue;
					} else {
						/** Force a text conversion to simplfy life later. */
						outputText[valuesIndex].set(outputValue.toString());
						values[valuesIndex] = outputText[valuesIndex];
					}
					valuesIndex++;
					reporter.incrCounter("DataJoinReduceOutput", "Output Keys", 1);
				}
			}
			values[valuesIndex] = null;
			if (valuesIndex>1) {
				/** If only one, no reason to bother sorting. */
				Arrays.sort( values, 0, valuesIndex, comparator );
			}
			for ( int i = 0; i < valuesIndex; i++ ) {
				if( LOG.isDebugEnabled()) {LOG.debug( String.format( "Output of %d of %d, %s %s", i, size, key, values[i])); }
				output.collect( key, values[i] );
			}
		
		} catch( Throwable e) {
			throwsIOExcepction(Reporter.NULL, "DataJoinMergeMapper", e);
		}
		
	}
	
	/** Find the first tab in <code>b</code> starting at <code>s</code>, and with the last index <code>max</code>.
	 * The expected value format is IP tab IP tab network name tab other data.
	 * This basically returns the index of the next tab character, or -1 if there are no more tabs.
	 *
	 * @param b The byte array to search
	 * @param s Start looking at index s
	 * @param max stop looking before max.
	 * @return the tab index or -1 if not found
	 */
	public static int findTab( final byte[] b, int s, final int max) {
		for( ; s < max; s++ ) {
			if (b[s]=='\t') {
				return s;
			}
		}
		return -1;
	}

	/** This expects and requires the value to be IPv4 TAB IPv4 TaB network TAB rest.
	 * the the comparison order is addr1, add2, network
	 */
	public static class TabbedNetRangeComparator implements Comparator<Text> {

		/** The comparator from the {@link Text} class, used for comparing the network names. */
		Text.Comparator comparator = new Text.Comparator();


		/** This expects and requires the value to be IPv4TABIPv4TaBnetworkTABline.
		 * the the comparison order is addr1, add2, network
		 *
		 * @param a Text value 1
		 * @param b Text value 2
		 * @return -1 1 or 0 less, greater or equal, the first item with a parse failure is considered greater.
		 */
		@Override
		public int compare( Text a, Text b ) {
			if( LOG.isDebugEnabled()) {LOG.debug( String.format("Comparing %s and %s", a, b)); }

			/** Do the basic check on <code>a</code>, see if we find the first bit. */
			final byte[] ab = a.getBytes();
			final int al = a.getLength();
			final int at1 = findTab( ab, 0, al );
			if (at1==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %s failed to find first tab", a)); }
				return 1;
			}

			/** Do the basic check on <code>b</code>, see if we find the first bit. */
			final byte[] bb = b.getBytes();
			final int bl = b.getLength();
			final int bt1 = findTab( bb, 0, bl );
			if (bt1==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("b %s failed to find first tab", b)); }
				return -1;
			}

			/** Get the first ip address from <code>a</code>. */
			final long aip1 = IPv4TextComparator.unpack( ab, 0, at1 );
			if (aip1==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %s failed to unpack %s", a, new String( ab, 0, at1))); }
				return 1;
			}

			/** Get the first ip address from <code>b</code>. */
			final long bip1 = IPv4TextComparator.unpack( bb, 0, bt1 );
			if (bip1==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("b %s failed to unpack %s", b, new String( bb, 0, bt1))); }
				return -1;
			}
			if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %x b%x", aip1, bip1)); }

			/** do the ip address comparison on the first IP, if they are different, this routine is done.
			 * Since we have longs and the result is int, a simple different not work.
			 */
			if (aip1<bip1) {
				return -1;
			}
			if (aip1>bip1) {
				return 1;
			}


			/** check the second IP address in <code>a</code> and <code>b</code> */
			
			final int at2 = findTab( ab, at1+1, al);
			if (at2==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %s failed to find second tab", a)); }
				return 1;
			}

			final long aip2 = IPv4TextComparator.unpack( ab, at1+1, at2 );
			if (aip2==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %s failed to unpack %s", a, new String( ab, at1+1, at2))); }
				return 1;
			}

			final int bt2 = findTab( bb, bt1+1, bl);
			if (bt2==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("b %s failed to find second tab", b)); }
				return -1;
			}
			final long bip2 = IPv4TextComparator.unpack( bb, bt1+1, bt2 );
			if (bip2==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("b %s failed to unpack %s", b, new String( bb, bt1+1, bt2))); }
				return -1;
			}
			if (aip2<bip2) {
				return -1;
			}
			if (aip2>bip2) {
				return 1;
			}

			/** At this point the both pairs of IP addresses are the same, pass the network names off to Text, which knows how to compare utf-8 bytes. */
			final int at3 = findTab( ab, at2+1, al);
			if (at3==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %s failed to find third tab", a)); }
				return 1;
			}

			final int bt3 = findTab( bb, bt2+1, al);
			if (bt3==-1) {
				if( LOG.isDebugEnabled()) {LOG.debug(String.format("b %s failed to find second tab", b)); }
				return -1;
			}
			if( LOG.isDebugEnabled()) {LOG.debug(String.format("a %s b %s", new String( ab, at2+1, at3), new String( bb, bt2+1, bt3))); }
			return comparator.compare( ab, at2+1, at3, bb, bt2+1, bt3 );

		}

		@Override
		public boolean equals(Object o) {
			if (o==null) {
				return false;
			}
			if (o==this) {
				return true;
			}
			if (o instanceof TabbedNetRangeComparator) {
				return true;
			}
			return false;
		}
			
	}


}
