package com.apress.hadoopbook.examples.ch9;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.apress.hadoopbook.ClusterMapReduceDelegate;

public class TestSimpleIPRangeParttioner extends ClusterMapReduceDelegate {

	public static Logger LOG = Logger.getLogger(SimpleIPRangePartitioner.class);
	
	static char rangeSeparator = ':';
	static char searchRequestSuffix = ';';
	static String searchSpaceRecordFile = "searchSpaceRecords";

	static class KeyValue {
		final String key;
		final String value;

		public KeyValue( final Text key, final Text value ) {
			this.key = key.toString();
			this.value = value.toString();
		}
		public KeyValue( final String key, final String value ) {
			this.key = key;
			this.value = value;
		}

	};
	
	static ArrayList<KeyValue> inputRecords = new ArrayList<KeyValue>();
	static ArrayList<KeyValue> outputRecords = new ArrayList<KeyValue>();

	static class TestOutputCollector implements OutputCollector<Text,Text> {
		@Override
		public
			void collect( Text key, Text value ) throws IOException
		{
			outputRecords.add( new KeyValue( key, value ) );
		}
	};
	
	
	/** These are the key descriptors, and the value in the record is the name of the object. */
	static class ExpectedPartitions {
		/** The name of this key, and the index in the hashmap. */
		String name;
		/** The beginning address of this search space key */
		long realBegin;
		/** The ending address of this search space key */
		long realEnd;
		/** The pairs of begin and ends for each partition, the array index is the partition number. */
		long [] begins;
		long [] ends;
		
		/** Construct an ExpectedPartitions object and store it in {@link TestSimpleIPRangeParttioner#keyToPartitionsMap}
		 * 
		 * @param name The name
		 * @param realBegin The actual begin address
		 * @param realEnd The actual end address
		 * @param begins the per partition begin addresses, a -1 indicates no value in this partition.
		 * @param ends The per partition end addresses. a -1 indicates no value in this partition.
		 */
		public ExpectedPartitions( final String name, 
				final long realBegin,	final long realEnd, 
				long[] begins, long[] ends 
				) {
			this.name = name;
			this.realBegin = realBegin;
			this.realEnd = realEnd;

			if ((this.realBegin&0x00000000)!=0 || this.realBegin<0) {
				throw new IllegalStateException(String.format("Begin Range is invalid %x", this.realBegin));
			}
			if ((this.realEnd&0x00000000)!=0 || this.realEnd<0) {
				throw new IllegalStateException(String.format("End Range is invalid %x", this.realEnd));
			}
			this.ends = ends;
			this.begins = begins;
			if (keyToPartitionsMap.containsKey(this.name)) {
				throw new IllegalStateException( String.format( "Duplicate Key name %s found on %d->%d", this.name, realBegin, realEnd) );
			}
			keyToPartitionsMap.put(this.name, this);
		}

		public void record()
		{
			inputRecords.add( new KeyValue( String.format("%08x%c%08x", realBegin, rangeSeparator, realEnd) ,name ) );
		}
		/** @return the ip.ipTABname suitable to be parsed by a key helper
		 * 
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			String res = String.format("%08x%c%08x\t%s", realBegin, rangeSeparator, realEnd, name );
			LOG.debug(res);
			return res;
// 			return ""
// 			+ ((realBegin>>24)&0xff)
// 			+ '.'
// 			+ ((realBegin>>16)&0xff)
// 			+ '.'
// 			+ ((realBegin>> 8)&0xff)
// 			+ '.'
// 			+ ((realBegin )&0xff)
// 			+ ':'
// 			+ ((realEnd>>24)&0xff)
// 			+ '.'
// 			+ ((realEnd>>16)&0xff)
// 			+ '.'
// 			+ ((realEnd>> 8)&0xff)
// 			+ '.'
// 			+ ((realEnd )&0xff)
// 			+ '\t'
// 			+ name
// 			;
					
		}
	};
	
	/** The search space records are output with a value that is the name of the {@link ExpectedPartitions} object
	 * that defines what partitions we expect to be populated for the search space record.
	 * This map provides a mapping between the record values and the {@link ExpectedPartitions} objects.
	 * Note: The value same as the {@link ExpectedPartitions#name}.
	 */
	static HashMap<String,ExpectedPartitions> keyToPartitionsMap = new HashMap<String,ExpectedPartitions>();

	public static final String OUTPUT_DIR_NAME = "spanSpaceKeys";
	
	/** Assume 4 reduces and setup a test data set to verify that all of the boundary
	 * conditions are handled correctly.
	 * The expected partitions, and begin/ends of the keys are packed into the value so that
	 * the results can be verified.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public static void generateTestData() throws IOException {
		Logger.getLogger("com.apress").setLevel(Level.ALL);
		
		final long[] partitionBeginnings = new long[] { 0x0L,	0x40000001L,	0x80000001L,	0xC0000001L };
		final long[] partitionEndings = new long[] { 0x40000000L,	0x80000000L,	0xC0000000L,	0xFFFFFFFFL };
		(new ExpectedPartitions( "FullIPV4", 0, 0xffffffffL, 
				partitionBeginnings,
				partitionEndings )).record();


		/** Spans each range fully */
		{
			long[] begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			long []ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0];
			ends[0] = partitionEndings[0];

			new ExpectedPartitions( "full span p0", begins[0], ends[0],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = 1;
			ends[0] = partitionEndings[0];
			new ExpectedPartitions( "p0 +1 to end", begins[0], ends[0],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = 1;
			ends[0] = partitionEndings[0] -1;
			new ExpectedPartitions( "p0 +1 to end -1", begins[0], ends[0],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0];
			ends[0] = partitionEndings[0] -1;
			new ExpectedPartitions( "p0 to end - 1", begins[0], ends[0],
												begins, ends ).record();


			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0];
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionBeginnings[1];
			new ExpectedPartitions( "p0 to p1", begins[0], ends[1],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0];
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionBeginnings[1]+1;
			new ExpectedPartitions( "p0 to p1+1", begins[0], ends[1],
												begins, ends ).record();


			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0] + 1;
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionBeginnings[1];
			new ExpectedPartitions( "p0+1 to p1", begins[0], ends[1],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0] + 1;
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionBeginnings[1]+1;
			new ExpectedPartitions( "p0+1 to p1+1", begins[0], ends[1],
												begins, ends ).record();


			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0];
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionEndings[1];
			begins[2] = partitionBeginnings[2];
			ends[2] = partitionBeginnings[2]+1;
			new ExpectedPartitions( "p0 to p2+1", begins[0], ends[2],
												begins, ends ).record();


			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0]+1;
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionEndings[1];
			begins[2] = partitionBeginnings[2];
			ends[2] = partitionBeginnings[2];
			new ExpectedPartitions( "p0+1 to p2", begins[0], ends[2],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionBeginnings[0]+1;
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionEndings[1];
			begins[2] = partitionBeginnings[2];
			ends[2] = partitionBeginnings[2]+1;
			new ExpectedPartitions( "p0+1 to p2+1", begins[0], ends[2],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionEndings[0];
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionEndings[1];
			begins[2] = partitionBeginnings[2];
			ends[2] = partitionBeginnings[2]+1;
			new ExpectedPartitions( "p1-1 to p2+1", begins[0], ends[2],
												begins, ends ).record();

			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[0] = partitionEndings[0];
			ends[0] = partitionEndings[0];
			begins[1] = partitionBeginnings[1];
			ends[1] = partitionEndings[1];
			
			new ExpectedPartitions( "p1-1 to p2-1", begins[0], ends[1],
												begins, ends ).record();
			
		}			
		
		
		String [] beginnings = new String[] { "p0 begin", "p1 begin", "p2 begin", "p3 begin" };
		/** Push out a record for each partition beginning, -1 on the beginning, +1 on the beginning, -1:0, 0:+1 and -1:+1 */
		for( int i = 0; i < beginnings.length; i++ ) {
			/** +0 */
			long[] begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			begins[i] = partitionBeginnings[i];
			ExpectedPartitions b = new ExpectedPartitions( beginnings[i], partitionBeginnings[i], partitionBeginnings[i],
					begins, begins );
			b.record();
			/** +1 */
			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			begins[i] = partitionBeginnings[i]+1;
			b = new ExpectedPartitions( beginnings[i] + "+1", partitionBeginnings[i]+1, partitionBeginnings[i]+1,
					begins, begins );
			b.record();
			/** 0:+1 */
			begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			long []ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
			begins[i] = partitionBeginnings[i];
			ends[i] = partitionBeginnings[i]+1;
			b = new ExpectedPartitions( beginnings[i] + i + " 0:+1", partitionBeginnings[i], partitionBeginnings[i]+1,
					begins, ends);
			b.record();
			
			if (i>0) {
				/** -1 */
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				begins[i-1] = partitionBeginnings[i] - 1;
				b = new ExpectedPartitions( beginnings[i] + i + " -1", partitionBeginnings[i]-1, partitionBeginnings[i]-1,
						begins, begins );
				b.record();
				/** -1:0 */
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
				begins[i-1] = partitionEndings[i-1];
				ends[i-1] = partitionEndings[i-1];
				begins[i] = partitionBeginnings[i];
				ends[i] = partitionBeginnings[i];
				b = new ExpectedPartitions( beginnings[i] + i + " -1:0", partitionBeginnings[i]-1, partitionBeginnings[i],
						begins, ends );
				b.record();

			}
			
		}

		String [] endings= new String[] { "p0 end", "p1 end", "p2 end", "p3 end" };
		/** Push out a record for each partition ending, -1 on the ending, +1 on the ending, -1:+0, -0:+1 and -1:+1 */
		for( int i = 0; i < endings.length; i++ ) {
			/** +0 */
			long[] begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
			begins[i] = partitionEndings[i];
			ExpectedPartitions b = new ExpectedPartitions( endings[i], partitionEndings[i], partitionEndings[i],
					begins, begins );
			b.record();
			if (i>0) {
				/** -1 */
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				begins[i-1] = partitionEndings[i] - 1;
				b = new ExpectedPartitions( endings[i] + i + " -1", partitionEndings[i]-1, partitionEndings[i]-1,
						begins, begins );
				b.record();
			
				/** -1:+0 */
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				long []ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
				begins[i-1] = partitionEndings[i-1];
				ends[i-1] = partitionEndings[i-1];
				begins[i] = partitionBeginnings[i];
				ends[i] = partitionBeginnings[i];
				b = new ExpectedPartitions( endings[i] + i + " -1:+0", begins[i-1], ends[i],
											begins, ends );
				b.record();
			}
			if (i<3) {
				/** +1 */
				long []ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				begins[i] = partitionEndings[i]+1;
				ends[i] = partitionEndings[i]+1;
				b = new ExpectedPartitions( endings[i] + i + " +1", partitionEndings[i]+1, partitionEndings[i]+1,
											begins, ends );
				b.record();
				/** +1 */
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
				begins[i] = partitionEndings[i];
				ends[i] = partitionEndings[i];
				begins[i+1] = partitionBeginnings[i+1];
				ends[i+1] = partitionBeginnings[i+1];

				b = new ExpectedPartitions( endings[i] + i + " -0:+1", partitionEndings[i], partitionBeginnings[i+1],
											begins, ends);
				b.record();

			}
			if (i>1&&i<3) {
				/** -1:+1 */
				begins = new long[4]; begins[0] = -1; begins[1] = -1; begins[2] = -1; begins[3] = -1;
				long []ends = new long[4]; ends[0] = -1; ends[1] = -1; ends[2] = -1; ends[3] = -1;
				begins[i-1] = partitionEndings[i-1];
				ends[i-1] = partitionEndings[i-1];
				begins[i] = partitionBeginnings[i];
				ends[i] = partitionBeginnings[i]+1;
				b = new ExpectedPartitions( endings[i] + i + " -1:+1", begins[i-1], ends[i],
											begins, ends );
				b.record();
				
			}
			
		}
		
		
		
		
		
	}

	
	@Test
	public void testSpanSpaceKeys() throws IOException {

		SimpleIPRangePartitioner partitioner = new SimpleIPRangePartitioner();
		JobConf conf = new JobConf();
		conf.setNumReduceTasks(4);
		conf.set(PartitionedTextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, "" + rangeSeparator);
		conf.set(PartitionedTextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, "" + searchRequestSuffix);
		partitioner.configure(conf);

		assertEquals( "Incorrect number of partitions", 4L, (long) partitioner.ranges.size() );
		ExpectedPartitions fullIPv4 = keyToPartitionsMap.get("FullIPV4");
		assertNotNull("Can find Fullipv4 entry", fullIPv4 );
		assertNotNull("Partition 0 end", partitioner.ranges.get(fullIPv4.ends[0]));
		assertNotNull("Partition 1 end", partitioner.ranges.get(fullIPv4.ends[1]));
		assertNotNull("Partition 2 end", partitioner.ranges.get(fullIPv4.ends[2]));
		assertNotNull("Partition 3 end", partitioner.ranges.get(fullIPv4.ends[3]));
		
		assertEquals("Partition 0 end " + fullIPv4.ends[0], 0, partitioner.ranges.get(fullIPv4.ends[0]).longValue());
		assertEquals("Partition 1 end " + fullIPv4.ends[1], 1, partitioner.ranges.get(fullIPv4.ends[1]).longValue());
		assertEquals("Partition 2 end " + fullIPv4.ends[2], 2, partitioner.ranges.get(fullIPv4.ends[2]).longValue());
		assertEquals("Partition 3 end " + fullIPv4.ends[3], 3, partitioner.ranges.get(fullIPv4.ends[3]).longValue());
		
		Text outputKey = new Text();
		PartitionedTextKeyHelperWithSeparators helper = new PartitionedTextKeyHelperWithSeparators();
		helper.setConf(conf);
		Text key = new Text();
		Text value = new Text();
		OutputCollector<Text,Text> output = new TestOutputCollector();
		StringBuilder sb = new StringBuilder();
		Formatter fmt = new Formatter(sb);
		int totalCount=0;
		LOG.debug("There are " + inputRecords.size() + " records to process");
		for (KeyValue inputRecord : inputRecords) {
			key.set(inputRecord.key);
			value.set(inputRecord.value);
			assertTrue("Unable to parse valid key from " + key + " " + value, helper.getFromRaw(key));
			assertTrue("Unable to parse valid key from " + key + " " + value, helper.isValid());
			int count = partitioner.spanSpaceKeys( helper, outputKey, value, output, Reporter.NULL );
			totalCount+=count;
		}
		LOG.debug(String.format("keyToPartition has %d entries", keyToPartitionsMap.size()));
		LOG.debug(String.format("There are %d output records to process", outputRecords.size()));
		int readRecordCount=0;
		for (KeyValue outputRecord : outputRecords) {
			readRecordCount++;
			key.set( outputRecord.key );
			value.set( outputRecord.value );
			
			helper.getFromRaw(key);
			assertTrue("Parsed key is valid " + key, helper.isValid());
			ExpectedPartitions expected = keyToPartitionsMap.get(outputRecord.value);
			assertNotNull("Not Found in key value map, " + value, expected);
			assertEquals("Real Key Begin " + key, expected.realBegin, helper.getRealRangeBegin());
			assertEquals("Real Key End" + key, expected.realEnd, helper.getRealRangeEnd());
			boolean foundRangeMatch = false;
			for ( int i = 0; i < expected.begins.length; i++) {
				if (
					helper.getBeginRange()==expected.begins[i]
					&&
					helper.getEndRange()==expected.ends[i]
					) {
					foundRangeMatch=true;
					break;
				}
			}
			sb.setLength(0);
			for( int x = 0; x < expected.begins.length; x++) {
				if (expected.begins[x]==-1 && expected.ends[x]==-1) {
					continue;
				}
				fmt.format("%x:%x ", expected.begins[x], expected.ends[x]);
			}
			fmt.flush();
			assertTrue(String.format("A match for %s\t%s, %x:%x was not found in [%s] the expected set", key, value, helper.getBeginRange(), helper.getEndRange(), sb), foundRangeMatch);
		}
		
	}

	@Test
	public void testGetPartition() {
		SimpleIPRangePartitioner partitioner = new SimpleIPRangePartitioner();
		JobConf conf = new JobConf();
		conf.setNumReduceTasks(4);
		conf.set(PartitionedTextKeyHelperWithSeparators.EXAMPLES_CH9_RANGE_SEPARATOR_CHAR, "" + rangeSeparator);
		conf.set(PartitionedTextKeyHelperWithSeparators.EXAMPLES_CH9_SEARCH_SUFFIX_CHAR, "" + searchRequestSuffix);
		partitioner.configure(conf);

		assertEquals( "Incorrect number of partitions", 4L, (long) partitioner.ranges.size() );
		ExpectedPartitions fullIPv4 = keyToPartitionsMap.get("FullIPV4");
		assertNotNull("Can find Fullipv4 entry", fullIPv4 );
		assertNotNull("Partition 0 end", partitioner.ranges.get(fullIPv4.ends[0]));
		assertNotNull("Partition 1 end", partitioner.ranges.get(fullIPv4.ends[1]));
		assertNotNull("Partition 2 end", partitioner.ranges.get(fullIPv4.ends[2]));
		assertNotNull("Partition 3 end", partitioner.ranges.get(fullIPv4.ends[3]));
		
		assertEquals("Partition 0 end " + fullIPv4.ends[0], 0, partitioner.ranges.get(fullIPv4.ends[0]).longValue());
		assertEquals("Partition 1 end " + fullIPv4.ends[1], 1, partitioner.ranges.get(fullIPv4.ends[1]).longValue());
		assertEquals("Partition 2 end " + fullIPv4.ends[2], 2, partitioner.ranges.get(fullIPv4.ends[2]).longValue());
		assertEquals("Partition 3 end " + fullIPv4.ends[3], 3, partitioner.ranges.get(fullIPv4.ends[3]).longValue());

		Text key  = new Text();
		Text value = new Text();
		
		for( long i = 0; i < fullIPv4.begins.length; i++ ) {
			key.set(String.format("%08x%c", fullIPv4.begins[(int)i], searchRequestSuffix));
			assertEquals( "Incorrect partition for " + fullIPv4.begins[(int)i], i, (long) partitioner.getPartition(key,value,4));

			key.set(String.format("%08x%c", fullIPv4.begins[(int)i]+1, searchRequestSuffix));
			assertEquals( "Incorrect partition for " + fullIPv4.begins[(int)i]+1, i, (long) partitioner.getPartition(key,value,4));


			key.set(String.format("%08x%c", fullIPv4.ends[(int)i], searchRequestSuffix));
			assertEquals( "Incorrect partition for " + fullIPv4.ends[(int)i], i, (long) partitioner.getPartition(key,value,4));

			key.set(String.format("%08x%c", fullIPv4.ends[(int)i]-1, searchRequestSuffix));
			assertEquals( "Incorrect partition for " + fullIPv4.ends[(int)i], i, (long) partitioner.getPartition(key,value,4));
		}
	}


}
