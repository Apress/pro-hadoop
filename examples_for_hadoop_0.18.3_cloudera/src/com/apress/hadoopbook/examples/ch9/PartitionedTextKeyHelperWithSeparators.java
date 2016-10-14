package com.apress.hadoopbook.examples.ch9;

import java.io.UnsupportedEncodingException;
import java.util.Formatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/** -D range.key.helper=com.apress.hadoopbook.examples.ch9.PartitionedTextKeyHelperWithSeparators
 * 
 * @author Jason
 *
 */
public class PartitionedTextKeyHelperWithSeparators extends TextKeyHelperWithSeparators {
	
	protected long realRangeBegin;
	protected long realRangeEnd;
	protected boolean hasRealRange = false;
	
	byte[] rangeSeparatorBytes;
	
	int rangeSeparatorByteLength;
	int ourKeyLength;
	
	int beginRangeStartIndex;
	int beginRangeEndIndex;
	
	int endRangeStartIndex;
	int endRangeEndIndex;
	
	int realBeginRangeStartIndex;
	int realBeginRangeEndIndex;
	
	int realEndRangeStartIndex;
	int realEndRangeEndIndex;
	
	/**
	 * @return the realRangeBegin
	 */
	public long getRealRangeBegin() {
		return realRangeBegin;
	}

	public PartitionedTextKeyHelperWithSeparators() {
		super();
	}
	/**
	 * @param realRangeBegin the realRangeBegin to set
	 */
	public void setRealRangeBegin(long realRangeBegin) {
		setHasRealRange(true);
		this.realRangeBegin = realRangeBegin;
	}

	/**
	 * @return the realRangeEnd
	 */
	public long getRealRangeEnd() {
		return realRangeEnd;
		
	}

	/**
	 * @param realRangeEnd the realRangeEnd to set
	 */
	public void setRealRangeEnd(long realRangeEnd) {
		this.realRangeEnd = realRangeEnd;
		setHasRealRange(true);
	}

	/**
	 * @return the hasRealRange
	 */
	public boolean isHasRealRange() {
		return hasRealRange;
	}

	/**
	 * @param hasRealRange the hasRealRange to set
	 */
	public void setHasRealRange(boolean hasRealRange) {
		this.hasRealRange = hasRealRange;
	}

	public PartitionedTextKeyHelperWithSeparators(Configuration conf) {
		super(conf);
		setConf(conf);
	}
	
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.AbstractKeyHelper#setConf(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		super.setConf(conf);
		try {
			rangeSeparatorBytes = new String("" + rangeSeparator).getBytes("UTF-8");
		} catch( UnsupportedEncodingException neverAndFatal) {
			throw new IllegalArgumentException("UTF-8 encoding missing", neverAndFatal);
		}

		if (rangeSeparatorBytes.length==0) {
			throw new IllegalArgumentException("The range separator character must not be empty");
		}
		
		rangeSeparatorByteLength = rangeSeparatorBytes.length;

		ourKeyLength = addressLen * 4 + 3 * rangeSeparatorByteLength;
		
		beginRangeStartIndex = 0;
		beginRangeEndIndex = addressLen;
		
		endRangeStartIndex = beginRangeEndIndex + rangeSeparatorByteLength;
		endRangeEndIndex = endRangeStartIndex + addressLen;
		
		realBeginRangeStartIndex = endRangeEndIndex + rangeSeparatorByteLength;
		realBeginRangeEndIndex = realBeginRangeStartIndex + addressLen;
		
		realEndRangeStartIndex = realBeginRangeEndIndex + rangeSeparatorByteLength;
		realEndRangeEndIndex = realEndRangeStartIndex + addressLen;
		
	}

	/* This method looks for either 8 hexadecimal values followed by {@link #searchRequestSuffix} or
	 * 2 sets of 8 hexadecimal values separated by {@link #rangeSeparator}.
	 * If the same of the string representation of <code>raw</code> does not fit those constraints.
	 * the method will return false, and {@link #isValid()} will also return false.
	 * 
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#getFromRaw(java.lang.Object)
	 */
	@Override
	public boolean getFromRaw(Text raw) {

		/** This is a proxy for the actual string length. If this key is already in the correct format,
		 * it is entirely composed of single byte characters, and there for the byte length == the string length.
		 */
		final int rawByteLength = raw.getLength();
		hasRealRange=false;
		if (rawByteLength!=ourKeyLength) {	/** If this does not contain 4 addresses separated by a single byte character, bail. */
			return super.getFromRaw(raw);
		}
		isValid = false;
		byte[] rawBytes = raw.getBytes();	/** Only up to rawByteLength are valid. */
		if (!(validSeparator(rawBytes, beginRangeEndIndex)
				&&
				validSeparator(rawBytes, endRangeEndIndex)
				&&
				validSeparator(rawBytes, realBeginRangeEndIndex)
				) ) {
			return false;	/** raw does not have 3 valid instances of rangeSeparator at the expected positions, it can not be a valid key. */
		}
		setBeginRange(unpack(rawBytes,beginRangeStartIndex));
		setEndRange(unpack(rawBytes,endRangeStartIndex));
		
		setRealRangeBegin(unpack(rawBytes,realBeginRangeStartIndex));
		setRealRangeEnd(unpack(rawBytes,realEndRangeStartIndex));
		hasEndRange = true;
		hasRealRange = true;
		isValid = true;
		return true;
	}

	
	/** convert 8 bytes of hex characters into a long.
	 * This could be much simpler if we were willing to build a 256 element table for each hex byte. 
	 * @param rawBytes The byte array, which must have at least be <code>start + 8</code> bytes in length
	 * @param start The start point.
	 * @return
	 */
	public static long unpack(byte[] rawBytes, int start) {
		final long a = Character.digit( rawBytes[start]  , 16 );
		final long b = Character.digit( rawBytes[start+1], 16 );
		final long c = Character.digit( rawBytes[start+2], 16 );
		final long d = Character.digit( rawBytes[start+3], 16 );
		final long e = Character.digit( rawBytes[start+4], 16 );
		final long f = Character.digit( rawBytes[start+5], 16 );
		final long g = Character.digit( rawBytes[start+6], 16 );
		final long h = Character.digit( rawBytes[start+7], 16 );
		if (a < 0 || b < 0 || c < 0	|| d < 0
				|| e < 0 ||	f < 0 || g < 0 || h < 0
				) {
			throw new NumberFormatException( "Unable to parse " + new String( rawBytes) + " as a hexadecimal number");
		}
		return a << 28 | b << 24 | c << 20 | d << 16 | e << 12 | f << 8 | g << 4 | h;
	}

	/** Verify that the byte sequence starting at <code>start is identical to the rangeSeparator.
	 * 
	 * rawBytes is assumed to have at least {@link #rangeSeparatorByteLength} bytes available from <code>start</code>.
	 * 
	 * @param rawBytes The byte array to examine
	 * @param start where to start.
	 * @return
	 */
	protected boolean validSeparator(byte[] rawBytes, int start) {
		for (int i = 0; i < rangeSeparatorByteLength; i++) {
			if (rawBytes[start+i] != rangeSeparatorBytes[i]) {
				return false;
			}
		}
		return true;
	}

	
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#setToRaw(java.lang.Object)
	 */
	@Override
	public void setToRaw(Text raw) {
		if (!isValid) {
			return;
		}
		if (!hasRealRange) {
			super.setToRaw(raw);
			return;
		}
		Formatter fmt = keyFormatter.get();
		fmt.flush();
		StringBuilder sb = keyBuilder.get();
		sb.setLength(0);
		
		fmt.format( "%08x%c%08x%c%08x%c%08x", beginRangeOrKey, rangeSeparator, endRange, rangeSeparator, realRangeBegin, rangeSeparator, realRangeEnd );
		fmt.flush();
		raw.set(sb.toString());
	}
	public String toString() {
		if (isValid&&hasRealRange) {
			return String.format( "%08x%c%08x%c%08x%c%08x", beginRangeOrKey, rangeSeparator, endRange, rangeSeparator, realRangeBegin, rangeSeparator, realRangeEnd );
		} else if(isValid) {
			return super.toString();
		} else {
			return "";
		}
		
	}
		
	

}
