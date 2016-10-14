package com.apress.hadoopbook.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;

/** Comparator that considers keys equal if they are in the same group of 10.
 * actually if the quotents for the division of the keys by 10 are equal
 * @author Jason
 *
 */
public class GroupByLongGroupingComparator implements RawComparator<LongWritable> {
	/** The amount to use for the divisor in computing the comparison quotent.
	 * To alter this derive from this class and set it in the constructor.
	 */
	int groupBy = 10;
	/** compare the serializes longs at 1/10th their value.
	 * @see org.apache.hadoop.io.RawComparator#compare(byte[], int, int, byte[], int, int)
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	      final long left = LongWritable.Comparator.readLong(b1, s1);
	      final long right = LongWritable.Comparator.readLong(b2, s2);
	      final long diff = left/groupBy - right/groupBy;
	      if (diff==0) {
				return 0;
			}
			if (diff<0) {
				return -1;
			}
			return 1;
	}

	/** compare the serializes longs at 1/10th their value.
	 * @see org.apache.hadoop.io.RawComparator#compare(Object, Object)
	 */
	@Override
	public int compare(final LongWritable o1, final LongWritable o2) {
		final long diff = (o1.get()/groupBy - o2.get()/groupBy);
		if (diff==0) {
			return 0;
		}
		if (diff<0) {
			return -1;
		}
		return 1;
	}

}