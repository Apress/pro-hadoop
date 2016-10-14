package com.apress.hadoopbook.examples.ch9;

import java.util.Formatter;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class TextKeyHelperWithSeparators extends AbstractKeyHelper<Text> {
	/** The value looked up in the configuration for the search request suffix. appended to keys to ensure
	 * that they sort after a search space key that beings on the same address.
	 */
	public static final String EXAMPLES_CH9_SEARCH_SUFFIX_CHAR = "examples.ch9.search.suffix.char";
	/** The value looked up in the configuration to be used for separating the begin and end ranges of
	 * a search space key.
	 */
	public static final String EXAMPLES_CH9_RANGE_SEPARATOR_CHAR = "examples.ch9.range.separator.char";
	/** The character used to separate a begin from an end range. */
	char rangeSeparator;
	/** The suffix appended to a search request to force sorting order. */
	char searchRequestSuffix;
	/** The length of a hex encoded address. */
	final int addressLen = 8;
	protected static Logger LOG = Logger.getLogger(TextKeyHelperWithSeparators.class);
	
	public static boolean isLegalSeparator(char c) {
		switch (c) {
		case '0': case '1': case '2': case '3': 
		case '4': case '5':	case '6': case '7':
		case '8': case '9': case 'a': case 'b':
		case 'c': case 'd': case 'e': case 'f':
		case 'A': case 'B': case 'C': case 'D':
		case 'E': case 'F':
			return false;
		}
		/** It would be nice to impose some further constraints, but why? */
		return true;
		
	}

	/** No Op constructer for reflection
	 * 
	 */
	public TextKeyHelperWithSeparators() {
		super();
	}


	
	/** Initialize {@link #rangeSeparator} and {@link #searchRequestSuffix} from the configuration.
	 * This is the only time they are set.
	 * @param conf The configuration to lookup the parameters in. defaults are ':' and ';' respectively.
	 */
	public TextKeyHelperWithSeparators( Configuration conf ) {
		super( conf );
		setConf(conf);

	}


	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.AbstractKeyHelper#setConf(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);
		rangeSeparator = conf.get( TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, ":").charAt(0);
		searchRequestSuffix = conf.get(TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, ";").charAt(0);
		if (rangeSeparator>=searchRequestSuffix) {
			throw new IllegalStateException( "The characters defined for " +
					TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR + 
					" and " +
					TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR  +
					" are not ordered correctly " +
					" char [" + rangeSeparator + "] is not less than [" + searchRequestSuffix + "]");
		}
		if (!isLegalSeparator(rangeSeparator)) {
			throw new IllegalStateException( "The character specified for " +
					TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR +
					"[" + rangeSeparator + "] " +
					" must not be one of 0-9a-z" );
		}
		if (!isLegalSeparator(searchRequestSuffix)) {
			throw new IllegalStateException( "The character specified for " +
					TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR +
					"[" + searchRequestSuffix + "] " +
					" must not be one of 0-9a-z" );
		}
	}

	/**
	 * @return the rangeSeparator
	 */
	public char getRangeSeparator() {
		return rangeSeparator;
	}


	/**
	 * @return the searchRequestSuffix
	 */
	public char getSearchRequestSuffix() {
		return searchRequestSuffix;
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
		isValid = false;
		hasEndRange = false;
		String rawText = raw.toString();
		if (rawText.length()==(addressLen+1) && rawText.charAt(addressLen)==searchRequestSuffix) {
			String searchRequest = rawText.substring(0, addressLen);
			beginRangeOrKey = addrToLong(searchRequest);
			if(beginRangeOrKey<0) {
				LOG.error(String.format("Failed to parse %s from %s as a hex number", searchRequest, raw));
				return false;
			}
		} else if (rawText.length()==(addressLen*2+1) && rawText.charAt(addressLen)==rangeSeparator) {
			
			final String beginRange = rawText.substring( 0, addressLen);
			beginRangeOrKey = addrToLong(beginRange);
			final String endRangeString = rawText.substring(addressLen+1,addressLen*2+1);
			endRange = addrToLong(endRangeString);
			if(beginRangeOrKey<0 || endRange<0) {
				LOG.error(String.format("Failed to parse one of  %s, %s from %s as a hex number", beginRange, endRangeString, raw));
				return false;
			}
			
			/** Verify that the begin range is less or equal to the end */
			if (beginRangeOrKey>endRange) {
				if (LOG.isDebugEnabled()) { LOG.debug("key [" + rawText + "] length " + rawText.length() + " begin > end " + beginRangeOrKey + " " + endRange); }
				return false;
			}
			hasEndRange = true;
		} else {
			/** Give it a brute force try, just in case a 3 dot octet set was passed in
			 */
			String [] parts = rawText.split(Pattern.quote(""+rangeSeparator));
			if (parts.length==1 || parts.length==2) {
				beginRangeOrKey = addrToLong(parts[0]);
				if (parts.length==2) {
					endRange = addrToLong(parts[1]);
				}
			}
			if (
					parts.length>2 || parts.length==0
					||
					(parts.length==1 || parts.length==2 && beginRangeOrKey<0)
					||
					(parts.length==2 && endRange<0)
					) {
				if (LOG.isDebugEnabled()) { LOG.debug(String.format("Can't parse %s as an ip address or address pare", rawText)); }
				return false;
			}
		}
		isValid = true;
		return true;
	}

	/** There is expected to be a large number of calls to the methods that repack key data
	 * These methods need to build up strings, and a {@link StringBuilder} is ideal.
	 * The choices are make a new StringBuilder in each call, have a StringBuilder local field and synchronize
	 * all of the users of the StringBuilder, or make a {@link ThreadLocal} instance.
	 * 
	 * The author likes thread local variables..
	 */
	ThreadLocal<StringBuilder> keyBuilder = new ThreadLocal<StringBuilder>() {
		@Override
		protected StringBuilder initialValue() {
			return new StringBuilder();
		}
	};
	/** This uses {@link #keyBuilder} under the covers for formatting key data.
	 */
	ThreadLocal<Formatter> keyFormatter = new ThreadLocal<Formatter>() {
		@Override
		protected Formatter initialValue() {
			return new Formatter(keyBuilder.get());
		}
	};
	
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.examples.ch9.KeyHelper#setToRaw(java.lang.Object)
	 */
	@Override
	public void setToRaw(Text raw) {
		if (!isValid) {
			return;
		}
		Formatter fmt = keyFormatter.get();
		fmt.flush();
		StringBuilder sb = keyBuilder.get();
		sb.setLength(0);
		
		if(hasEndRange) {
			fmt.format( "%08x%c%08x", beginRangeOrKey, rangeSeparator, endRange );
		} else {
			fmt.format( "%08x%c", beginRangeOrKey, searchRequestSuffix);
		}
		fmt.flush();
		raw.set(sb.toString());
	}
	public String toString() {
		if(!isValid) {
			return "";
		}
		if(hasEndRange) {
			return String.format( "%08x%c%08x", beginRangeOrKey, rangeSeparator, endRange );
		} else {
			return String.format( "%08x%c", beginRangeOrKey, searchRequestSuffix);
		}
		
	}



}
