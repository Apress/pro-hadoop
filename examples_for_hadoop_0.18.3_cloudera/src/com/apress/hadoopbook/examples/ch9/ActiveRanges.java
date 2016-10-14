/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import com.sun.jmx.remote.internal.ArrayQueue;

class ActiveRanges {
	public static class Range<V> {
		long end;
		long begin;
		V value;
		Range( long begin, final long end, final V value) {
			this.begin = begin;
			this.end = end;
			this.value = value;
		}
		V getValue() {
			return value;
		}
		void setEnd( long end ) {
			this.end = end;
		}	

		void setBegin( long begin ) {
			this.begin = begin;
		}
		void setValue( V value ) {
			this.value = value;
		}
		void set( long begin, long end, V value ) {
			this.begin = begin;
			this.end = end;
			this.value = value;
		}
		public long getEnd() {
			return end;
		}
		public long getBegin() {
			// TODO Auto-generated method stub
			return begin;
		}
	};

	/** Keep up to 100 pairs around. This is a guess for the start, the counters will tell us
	 * how to tune this.
	 * The locking and waiting functions are overkill, all we need are the fixed size queue
	 * that doesn't allocate and deallocate for each add/remove.
	 */
	ArrayBlockingQueue<ActiveRanges.Range<String>> pairPool = new ArrayBlockingQueue<ActiveRanges.Range<String>>(100);

	/** This queue is unbounded. Lazy picking up an internal class rather than implementing one. */
	ArrayQueue<ActiveRanges.Range<String>> activeRanges = 
		new ArrayQueue<ActiveRanges.Range<String>>(1000);

	public void deactivate(long end) {
		while (!activeRanges.isEmpty()) {

			ActiveRanges.Range<String> pair = activeRanges.get(0);

			/** If this range is no longer valid, remove it, and put the end object back in the pool. */
			if (pair.getEnd() < end) {
				activeRanges.remove(0);
				pairPool.add(pair);
			} else {
				return;
			}

		}
	}

	public int size() {
		return activeRanges.size();
	}

	public boolean isEmpty() {
		return activeRanges.isEmpty();
	}

	public ActiveRanges.Range<String> get(int i) {
		return activeRanges.get(i);
	}

	public void activate( final Reporter reporter, final String tag, KeyHelper<Text> helper, final String value ) {
		ActiveRanges.Range<String> pair = pairPool.poll();
		long begin;
		long end;
		if (helper instanceof PartitionedTextKeyHelperWithSeparators) {
			begin = ((PartitionedTextKeyHelperWithSeparators)helper).getRealRangeBegin();
			end = ((PartitionedTextKeyHelperWithSeparators)helper).getRealRangeEnd();
		} else {
			begin = helper.getBeginRange();
			end = helper.getEndRange();
		}
		if (pair==null) {
			pair = new ActiveRanges.Range<String>( begin, end, value);
		} else {
			pair.set( begin, end, value);
		}
		if (pair.getBegin()==0) {
			int i = 0;
			i++;
		}
		try { /** There is no capacity method, this is the only way to find out if it is full. */
			activeRanges.add( pair );
		} catch( ArrayIndexOutOfBoundsException needToResize ) {
			reporter.incrCounter(tag, "Resize Active Ranges", 1);
			activeRanges.resize( activeRanges.size() + 1000);
			activeRanges.add( pair );
		}
		reporter.incrCounter(tag, "Range Histogram: " + activeRanges.size(), 1);
	}


	public void clear() {
		int oldSize = activeRanges.size();
		activeRanges.clear();
		if (oldSize>1000) {
			activeRanges.resize(1000);
		}
	}
}