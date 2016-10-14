package com.apress.hadoopbook.examples.ch9;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public abstract class AbstractKeyHelper<K> implements KeyHelper<K>, Configurable {
	/** Saved configuration. Used for constructing key objects. */
	Configuration conf;
	/** The beingRange or search request key. */
	long beginRangeOrKey;
	/** The endRange */
	long endRange;
	/** If true, endRange is valid and this object contains a search space key. */
	boolean hasEndRange = false;
	/** if true, this object represents a valid key. */
	boolean isValid = false;
	

	public AbstractKeyHelper( Configuration conf) {
		this.conf = conf;
		
		
	}
	/** Nop for reflection
	 * 
	 */
	public AbstractKeyHelper() {

	}
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#getBeginRange()
	 */
	@Override
	public long getBeginRange() {
		return beginRangeOrKey;
	}

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#getEndRange()
	 */
	@Override
	public long getEndRange() {
		return endRange;
	}

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#getSearchRequest()
	 */
	@Override
	public long getSearchRequest() {
		return beginRangeOrKey;
	}

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#isSearchRequest()
	 */
	@Override
	public boolean isSearchRequest() {
		return isValid && !hasEndRange;
	}

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#isSearchSpace()
	 */
	@Override
	public boolean isSearchSpace() {
		// TODO Auto-generated method stub
		return isValid && hasEndRange;
	}

	@Override
	public boolean isValid() {
		return isValid;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.conf.Configurable#getConf()
	 */
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}
	
	/** set the {@link #beginRangeOrKey} and {@link #endRange} to -1L and {@link #isValid} to false.
	 * 
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#reset()
	 */
	@Override
	public void reset() {
		beginRangeOrKey = -1;
		endRange = -1;
		isValid = false;
	}

	/** Set the {@link #beginRangeOrKey}, this will also set {@link #hasEndRange} true and {@link #isValid} true.
	 * 
	 * @param beginRange
	 */
	public void setBeginRange(final long beginRange) {
		this.beginRangeOrKey = beginRange;
		isValid = true;
		hasEndRange = true;
	}
	
	/** Set the {@link #endRange}, this will also set {@link #hasEndRange} true and {@link #isValid} true.
	 * @param endRange
	 */
	public void setEndRange(final long endRange) {
		this.endRange = endRange;
		isValid = true;
		hasEndRange = true;
	}
	
	/** Set the {@link #beginRangeOrKey}, this will also set {@link #hasEndRange} false and {@link #isValid} true.
	 * @param searchRequest
	 */
	public void setSearchRequest(final long searchRequest) {
		this.beginRangeOrKey = searchRequest;
		isValid = true;
		hasEndRange = false;
	}
	/** Take an input String either a 8 digit hex number or a IPv4 3 dot octet set
	 * and return the 32bit value it represents in a long
	 * 
	 * @param ipAddress The string containing an ip address
	 * @return the numeric value of the address or -1 for parse failure.
	 */
	public static long addrToLong(final String ipAddress) {
		if (ipAddress.length()==7||ipAddress.length()==8) {/** Could be a hex address */
			try {
				/** If it parses, we are done, otherwise fall through to the main block. */
				return Long.parseLong(ipAddress,16);
			} catch( NumberFormatException ignore) {}
		}
		InetAddress []addresses = null;
		UnknownHostException badHost = null;
		try {
			addresses = InetAddress.getAllByName(ipAddress);
		} catch( UnknownHostException e ) {
			badHost = e;
		}
		if (badHost!=null || addresses.length!=1 || !(addresses[0] instanceof Inet4Address)) {
			return -1;
		}
		byte[] addr = ((Inet4Address) addresses[0]).getAddress();
		long a = (long) (addr[0] & 0xff);
		long b = (long) (addr[1] & 0xff);
		long c = (long) (addr[2] & 0xff);
		long d = (long) (addr[3] & 0xff);
		long address = d;
		address |= ((c <<  8) & 0xFF00);
		address |= ((b << 16) & 0xFF0000);
		address |= ((a << 24) & 0xFF000000);
		address &= 0xFFFFFFFF;
		return address;
	}

	
}
