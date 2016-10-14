package com.apress.hadoopbook.examples.ch9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** Comparator that considers keys equal if they are in the same group of 10.
 * actually if the quotents for the division of the keys by 10 are equal
 * @author Jason
 *
 */
public class IPv4TextComparator extends WritableComparator {
	
	protected IPv4TextComparator(Class<? extends WritableComparable<Text>> keyClass) {
		super(keyClass);
		// TODO Auto-generated constructor stub
	}

	protected IPv4TextComparator(Class<? extends WritableComparable<Text>> keyClass,
			boolean createInstances) {
		super(keyClass, createInstances);
		// TODO Auto-generated constructor stub
	}

	public IPv4TextComparator()
	{
		super(Text.class);
	}
	/** compare the serialized form of two text objects containing IPv4 addresses
	 * of the form 0.0.0.0 through 255.255.255.255.
	 * @see org.apache.hadoop.io.RawComparator#compare(byte[], int, int, byte[], int, int)
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	     long a1 = unpack( b1, s1, l1 );
	     long a2 = unpack( b2, s2, l2 );
		 if (a1<a2) {
			 return -1;
		 }
		 if (a1>a2) {
			 return 1;
		 }
		 return 0;
	}

	/** Given a byte buffer that contains a standard decimal dotted octet IPv4 address
	 * (ie: 0.0.0.0 through 255.255.255.255), as a byte stream, return the long value
	 * of the ip address
	 * 
	 * @param buf The byte buffer containing the bytes.
	 * @param s	The start address in <code>buf</code>.
	 * @param l	The length of data in <code>buf</code> to use.
	 * @return the numeric value of the address 0 -> 2^32, or -1 for parse errors.
	 */
	public static long unpack( final byte []buf, int s, int l) {
		long result = 0;
		long part = 0;
		l += s;
		for( ; s < l; s++ ) {
			byte b = buf[s];
			switch(b) {
			case '.':
				result <<= 8;
				result += part;
				part = 0;
				continue;
			case '0': case '1': case '2': case '3': case '4':
			case '5': case '6': case '7': case '8': case '9':
				part *= 10;
				part += Character.getNumericValue((int)b);
				continue;
			default:
				return -1;
			}
		}
		result <<= 8;
		result += part;
		return result;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		if (a instanceof Text && b instanceof Text) {
			return compare((Text)a, (Text)b);
		}
		return super.compare(a, b);
	}

	/** compare to text objects that are IPv4 addresses in dotted octet notation.
	 * @see org.apache.hadoop.io.RawComparator#compare(Object, Object)
	 */
	
	public int compare(final Text o1, final Text o2) {
		return compare( o1.getBytes(), 0, o1.getLength(),
				o2.getBytes(), 0, o2.getLength());
	}
	
	/** compare to text objects that are IPv4 addresses in dotted octet notation.
	 * @see org.apache.hadoop.io.RawComparator#compare(Object, Object)
	 */
	@Override
	public int compare(final Object a, final Object b) {
		if (a instanceof Text && b instanceof Text) {
			return compare((Text)a, (Text)b);
		}
		return super.compare(a, b);
	}

}
