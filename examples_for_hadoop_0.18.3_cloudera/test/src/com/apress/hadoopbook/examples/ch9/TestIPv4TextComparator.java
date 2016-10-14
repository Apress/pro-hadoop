/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.junit.Test;

/** Verify that the IPv4 Comparator is actually working correctly.

 * @author Jason
 *
 */
public class TestIPv4TextComparator {
	/** Try various addresses and addresses embedded in text to ensure that unpack works correctly
	 * We know that under the covers Text objects are serialized as UTF-8.
	 * Test method for {@link com.apress.hadoopbook.examples.ch9.IPv4TextComparator#unpack(byte[], int, int)}.
	 * @throws UnsupportedEncodingException 
	 */
	@Test
	public void testUnpack() throws UnsupportedEncodingException {
		byte[] zero = "0.0.0.0".getBytes("UTF-8");
		assertEquals("0.0.0.0", 0L, IPv4TextComparator.unpack( zero, 0, zero.length ));
		byte[] bcast = "255.255.255.255".getBytes("UTF-8");
		assertEquals("255.255.255.255", 4294967295L, IPv4TextComparator.unpack( bcast, 0, bcast.length ));
		byte[] twos = "10.11.12.13".getBytes("UTF-8");
		assertEquals("10.11.12.13", 168496141L, IPv4TextComparator.unpack( twos, 0, twos.length ));
		
		byte[] acend = "2.13.234.1".getBytes("UTF-8");
		assertEquals("2.13.234.1", 34466305L, IPv4TextComparator.unpack( acend, 0, acend.length ));

		byte[] acend1 = "2.13.234.2".getBytes("UTF-8");
		assertEquals("2.13.234.2", 34466306L, IPv4TextComparator.unpack( acend1, 0, acend1.length ));

		byte[] desc = "234.42.13.0".getBytes("UTF-8");
		assertEquals("234.42.13.0", 3928624384L, IPv4TextComparator.unpack( desc, 0, desc.length ));
					 

		byte[] adesc = "a234.42.13.0".getBytes("UTF-8");
		assertEquals("a234.42.13.0", 3928624384L, IPv4TextComparator.unpack( adesc, 1, adesc.length-1 ));

		byte[] adescb = "a234.42.13.0b".getBytes("UTF-8");
		assertEquals("a234.42.13.0b", 3928624384L, IPv4TextComparator.unpack( adescb, 1, adescb.length-2 ));

	}


	/** Compare various actual addresses.
	 * Test method for {@link com.apress.hadoopbook.examples.ch9.IPv4TextComparator#compare(org.apache.hadoop.io.Text, org.apache.hadoop.io.Text)}.
	 */
	@Test
	public void testCompareTextText() {
		Text left = new Text();
		Text right = new Text();
		IPv4TextComparator comparator = new IPv4TextComparator();
		
		left.set("0.0.0.0");
		right.set("0.0.0.1");
		assertTrue( left + " less than " + right, comparator.compare(left,right) < 0 );
		assertTrue( right + " less than " + left, comparator.compare(right,left) > 0 );
		assertTrue( left + " equals " + left, comparator.compare(left,left) == 0 );
		assertTrue( right + " equals " + right, comparator.compare(right,right) == 0 );
		left.set("0.0.0.0");
		right.set("255.255.255.255");
		assertTrue( left + " less than " + right, comparator.compare(left,right) < 0 );
		assertTrue( right + " less than " + left, comparator.compare(right,left) > 0 );
		assertTrue( left + " equals " + left, comparator.compare(left,left) == 0 );
		assertTrue( right + " equals " + right, comparator.compare(right,right) == 0 );
	}


	/** Try actual comparions including addresses as sub regions
	 * Test method for {@link com.apress.hadoopbook.examples.ch9.IPv4TextComparator#compare(byte[], int, int, byte[], int, int)}.
	 */
	@Test
	public void testCompareByteArrayIntIntByteArrayIntInt() {


		Text left = new Text();
		Text right = new Text();
		IPv4TextComparator comparator = new IPv4TextComparator();
		
		left.set("a0.0.0.0b");
		right.set("c0.0.0.1d");
		assertTrue( left + " less than " + right, comparator.compare(left.getBytes(), 1, left.getLength()-2,right.getBytes(), 1, right.getLength()-2) < 0 );
		assertTrue( right + " less than " + left, comparator.compare(right.getBytes(), 1, right.getLength()-2,left.getBytes(), 1, left.getLength()-2) > 0 );
		assertTrue( left + " equals " + left, comparator.compare(left.getBytes(), 1, left.getLength()-2,left.getBytes(), 1, left.getLength()-2) == 0 );
		assertTrue( right + " equals " + right, comparator.compare(right.getBytes(), 1, right.getLength()-2,right.getBytes(), 1, right.getLength()-2) == 0 );
		left.set("e0.0.0.0f");
		right.set("g255.255.255.255h");
		assertTrue( left + " less than " + right, comparator.compare(left.getBytes(), 1, left.getLength()-2,right.getBytes(), 1, right.getLength()-2) < 0 );
		assertTrue( right + " less than " + left, comparator.compare(right.getBytes(), 1, right.getLength()-2,left.getBytes(), 1, left.getLength()-2) > 0 );
		assertTrue( left + " equals " + left, comparator.compare(left.getBytes(), 1, left.getLength()-2,left.getBytes(), 1, left.getLength()-2) == 0 );
		assertTrue( right + " equals " + right, comparator.compare(right.getBytes(), 1, right.getLength()-2,right.getBytes(), 1, right.getLength()-2) == 0 );


		left.set("eee0.0.0.0");
		right.set("g255.255.255.255");
		assertTrue( left + " less than " + right, comparator.compare(left.getBytes(), 3, left.getLength()-3,right.getBytes(), 1, right.getLength()-2) < 0 );
		assertTrue( right + " less than " + left, comparator.compare(right.getBytes(), 1, right.getLength()-2,left.getBytes(), 3, left.getLength()-3) > 0 );
		assertTrue( left + " equals " + left, comparator.compare(left.getBytes(), 3, left.getLength()-3,left.getBytes(), 3, left.getLength()-3) == 0 );
		assertTrue( right + " equals " + right, comparator.compare(right.getBytes(), 1, right.getLength()-2,right.getBytes(), 1, right.getLength()-2) == 0 );

		
	}



}
