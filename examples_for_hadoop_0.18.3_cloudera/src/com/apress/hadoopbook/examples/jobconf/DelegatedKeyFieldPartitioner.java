/**
 * 
 */
package com.apress.hadoopbook.examples.jobconf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;



public class DelegatedKeyFieldPartitioner<K,V> implements Partitioner<K,V> {
	static final Log LOG = LogFactory.getLog(DelegatedKeyFieldPartitioner.class);
	static class KeyWrapper<K,V> extends KeyFieldBasedPartitioner<K,V> {
		
		@Override
		protected int hashCode(byte[] b, int start, int end, int currentHash) {
			OutputComparatorAndGrouping.LOG.info("Working on " + new String(b) + " " + b.length + " start " + start + " end " + end);
			try {
				return super.hashCode( b, start, end, currentHash );
			} catch( Throwable e) {
				OutputComparatorAndGrouping.LOG.error( "Failed to hash", e);
				if (e instanceof RuntimeException) {
					throw (RuntimeException) e;
				}
				throw new RuntimeException( "failed to cast",e);
			}
		  }

	}
	KeyFieldBasedPartitioner<K, V> real;

	public DelegatedKeyFieldPartitioner() {
		OutputComparatorAndGrouping.LOG.info( "Starting partitioner");
		real= new DelegatedKeyFieldPartitioner.KeyWrapper<K,V>();
	}

	/**
	 * @param job
	 * @see org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner#configure(org.apache.hadoop.mapred.JobConf)
	 */
	public void configure(JobConf job) {
		real.configure(job);
	}
	
	/**
	 * @param obj
	 * @return
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		return real.equals(obj);
	}

	/**
	 * @param key
	 * @param value
	 * @param numReduceTasks
	 * @return
	 * @see org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	public int getPartition(K key, V value, int numReduceTasks) {
		OutputComparatorAndGrouping.LOG.info("Working on key " + key + " and value " + value + "num reduces " + numReduceTasks);
		try {
			return real.getPartition(key, value, numReduceTasks);
		} catch( Throwable e ) {
			OutputComparatorAndGrouping.LOG.error( "Failed to partion " + key + ", " + value );
			return 1;
		}
	}

	/**
	 * @return
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return real.hashCode();
	}

	/**
	 * @return
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return real.toString();
	}
}