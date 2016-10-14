/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Jason
 *
 */
public class TestTextKeyHelperWithSeparators {

	StringBuilder sb = new StringBuilder();
	Formatter fmt = new Formatter(sb);
	JobConf conf = new JobConf();
	
	TextKeyHelperWithSeparators helper;
	Text key = new Text();
	Text otherKey = new Text();
	/** Empty {@link #sb} before each method.
	 * 
	 */
	@Before
	public void before()
	{
		fmt.flush();
		sb.setLength(0);
		
		
		for( Iterator<Map.Entry<String,String>> it = conf.iterator(); it.hasNext(); ) {
			it.remove();
		}
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, "^");
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, "_");
		helper = new TextKeyHelperWithSeparators(conf);
		key.clear();
		otherKey.clear();
	}
	/**
	 * Test method for {@link com.apress.hadoopbook.examples.ch9.TextKeyHelperWithSeparators#TextKeyHelperWithSeparators(org.apache.hadoop.conf.Configuration)}.
	 */
	@Test
	public void testTextKeyHelperWithSeparators() {

		/** Check the basics. */
		assertNotNull("Have constructed a helper", helper);
		assertFalse("newly constructed helper is not valid", helper.isValid());
		assertFalse("Newly constructed helper is not a search space request key", helper.isSearchRequest());
		assertFalse("Newly constructed helper is not a search space key", helper.isSearchSpace());
		assertEquals( "Range Separator character correct", '^', helper.rangeSeparator );
		assertEquals( "Search Request Suffix character correct", '_', helper.searchRequestSuffix );

		
		/** These are in reverse sort order. */
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, ";");
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, ":");

		IllegalStateException wanted = null;
		try {
			helper = new TextKeyHelperWithSeparators(conf);
		} catch( IllegalStateException e) {
			wanted = e;
		}
		assertNotNull("Range check on separator characters failed to catch ordering failure", wanted);
		
		/** Check for failures to notice invalid separators. */
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, "a");
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, "z");
		wanted = null;
		try {
			helper = new TextKeyHelperWithSeparators(conf);
		} catch( IllegalStateException e) {
			wanted = e;
		}
		assertNotNull("Failed to notice invalid search suffix character", wanted);
		
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, " ");
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, "a");
		
		wanted = null;
		try {
			helper = new TextKeyHelperWithSeparators(conf);
		} catch( IllegalStateException e) {
			wanted = e;
		}
		assertNotNull("Failed to notice invalid range separator character", wanted);
		
		
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, "^");
		conf.set(TextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, "_");
		helper = new TextKeyHelperWithSeparators(conf);

		assertEquals( "Range Separator character correct", '^', helper.rangeSeparator );
		assertEquals( "Search Request Suffix character correct", '_', helper.searchRequestSuffix );

		
	}
	

	/** This verifies that the get set is the same as the key got.
	 * Relies on {@link TestTextKeyHelperWithSeparators#setAndTestKey(boolean, long, long, boolean, char, char)} for building  helper objects.
	 * 
	 * @param isRange Is is this a range key or a search request key
	 * @param begin The begin or key value as isRange.
	 * @param end The end value if this is <code>isRange</code> is true.
	 * @param isValid If this should be a valid transform.
	 * @param rangeSeparator The range separator character to use.
	 * @param searchRequestSuffix The search suffix to use.
	 */
	public void setAndTestKey(boolean isRange, long begin,
			long end, boolean isValid, char rangeSeparator, char searchRequestSuffix) {
		setAndTestHelper( isRange, begin, end, isValid, rangeSeparator, searchRequestSuffix);
		otherKey.clear();
		helper.setToRaw(otherKey);
		assertEquals("set key is correct", key, otherKey);
	}

	/** Build a key and initialize the helper with it and validate the helper.
	 * 
	 * @param isRange Is is this a range key or a search request key
	 * @param begin The begin or key value as isRange.
	 * @param end The end value if this is <code>isRange</code> is true.
	 * @param isValid If this should be a valid transform.
	 * @param rangeSeparator The range separator character to use.
	 * @param searchSuffix The search suffix to use.
	 */
	public void setAndTestHelper( boolean isRange, long begin, long end, boolean isValid, char rangeSeparator, char searchSuffix )
	{

		// Build the key text
		sb.setLength(0);
		if (isRange) {
			fmt.format("%08x%c%08x", begin, rangeSeparator, end );
		} else {
			fmt.format( "%08x%c", begin, searchSuffix );
		}
		fmt.flush();			// doing this is paranoia
		key.set(sb.toString());

		// load it into helper and validate
		assertEquals("Construction of helper success?", isValid, helper.getFromRaw( key ) );
		assertEquals( "Helper state correct after getFromRaw", isValid, helper.isValid() );
		if (isValid) {
			assertEquals("Key type Search Space correct", isRange, helper.isSearchSpace() );
			assertTrue( "Key type Search Range correct", isRange != helper.isSearchRequest() );
		}

		assertEquals( "Begin key correct", begin, isRange ? helper.getBeginRange() : helper.getSearchRequest() );

		if (isRange) {
			assertEquals( "End key correct", end, helper.getEndRange() );
		}

	}

	/**
	 * Test method for {@link com.apress.hadoopbook.examples.ch9.TextKeyHelperWithSeparators#getFromRaw(org.apache.hadoop.io.Text)}.
	 */
	@Test
	public void testGetFromRaw() {
		

		/** Test keys values at the boundaries and a few others. */
		long zero = 0;
		long maxUnsignedInt = 4294967296L;	/** 2 ^ 32 */
		long localHost = 0x7f000001;	/** 127.0.0.1 */
		long privateC = 0xc0a80001L;		/** 192.168.0.1 */
		


		setAndTestHelper( true, zero, maxUnsignedInt-1, true, helper.rangeSeparator, helper.searchRequestSuffix );

		setAndTestHelper( false, localHost, 0, true, helper.rangeSeparator, helper.searchRequestSuffix );

		/** Format a bad one */
		sb.setLength(0);
		fmt.format("%08x", privateC ); fmt.flush();
		key.set(sb.toString());
		assertFalse( "Helper failed to extract data from invalid search request", helper.getFromRaw(key));	/** Format a bad one */
		
		/** wrong suffix. */
		setAndTestHelper( false, localHost, 0, false, helper.searchRequestSuffix, helper.rangeSeparator );
		
		/** wrong separator. */
		setAndTestHelper( false, localHost, 0, false, helper.searchRequestSuffix, helper.rangeSeparator );


		setAndTestHelper( true, localHost, privateC, true, helper.rangeSeparator, helper.searchRequestSuffix );		

	}

	/**
	 * Test method for {@link com.apress.hadoopbook.examples.ch9.TextKeyHelperWithSeparators#setToRaw(org.apache.hadoop.io.Text)}.
	 */
	@Test
	public void testSetToRaw() {
		long zero = 0;
		long maxUnsignedInt = 4294967296L;	/** 2 ^ 32 */
		long localHost = 0x7f000001;	/** 127.0.0.1 */
		long privateC = 0xc0a80001L;		/** 192.168.0.1 */
		

		setAndTestKey( true, zero, maxUnsignedInt-1, true, helper.rangeSeparator, helper.searchRequestSuffix );

		setAndTestKey( false, localHost, 0, true, helper.rangeSeparator, helper.searchRequestSuffix );
		setAndTestKey( true, localHost, privateC, true, helper.rangeSeparator, helper.searchRequestSuffix );	
		
	}


}
