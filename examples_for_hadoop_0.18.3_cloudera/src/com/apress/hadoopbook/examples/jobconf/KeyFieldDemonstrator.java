/**
 * 
 */
package com.apress.hadoopbook.examples.jobconf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.Copy_2_of_KeyFieldBasedComparator;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.MainProgrameShell;

/** This class provides a working demonstration of what data is actually used when the {@link KeyFieldBasedPartitioner} and {@link KeyFieldBasedComparator} actually used.
 * It will probably only run from within an IDE such as eclipse, with the fatal errors for illegal accesses disabled.
 * 
 * @author Jason
 *
 */
public class KeyFieldDemonstrator extends MainProgrameShell {
	String outputLine = "1 23 456 89 abc efghi jklmno qrst\t1";
	public KeyFieldDemonstrator() {
	}
	public KeyFieldDemonstrator(String outputLine) {
		this.outputLine = outputLine;
	}
	static class KeyFieldEvent {
		boolean isPartition;
		String one;
		String two;
		String outputLine;
		private boolean isKeySpec;
		public KeyFieldEvent( byte [] b1, int s1, int l1)
		{
	
				try {
					one = new String( b1, s1, l1-s1+1, "UTF-8");
					System.out.println( String.format( "b %d s1 %d (%c) l1 %d len %d (%s) (%s)",
							b1.length, s1, (char) b1[s1], l1, l1-s1, new String(b1, "UTF-8"), one)); 
					
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println( "Partitioning on " + one);
	
			two = null;
			isPartition=true;
		}
		
		public KeyFieldEvent( byte [] b1, int s1, int l1, byte [] b2, int s2, int l2) {
			
				
				try {
					one = new String( b1, s1, l1-s1+1, "UTF-8");

					two= new String( b2, s2, l2-s2+1, "UTF-8" );
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.println( String.format( "Comparing on %s %d, %d, %d, %d and %s %d, %d, %d %d", one, b1.length, s1, l1, l1-s1+1, two, b2.length, s2, l2, l2-s2+1 ) );
			
			isPartition = false;
		}
		public KeyFieldEvent( String keyCode, String partitionCode ) {
			isPartition = false;
			one = keyCode;
			two = partitionCode;
		}
		public KeyFieldEvent( String outputLine, String keyCode, String partitionCode ) {
			isKeySpec = true;
			one = keyCode;
			two = partitionCode;
			this.outputLine = outputLine;
		}
		
		public String toString() {
			if (isKeySpec) {
				return "KeySpec[comparator: (" + one +"), partitioner: (" + two + "), input line: (" + outputLine + ")]";
			}
			if (isPartition) {
				return "Partitioner[key: (" + one + ")]";
			} else {
				return "Comparator[key: (" + one + "), key: (" + two +")]";
			}
		}
	};
	
	static List<KeyFieldEvent> events = Collections.synchronizedList(new ArrayList<KeyFieldEvent>());
	
	static class DelegatedComparator extends Copy_2_of_KeyFieldBasedComparator<Text, Text> {
		
		@Override
		protected int compareByteSequence(byte[] first, int start1, int end1, 
			      byte[] second, int start2, int end2, Object keyObj) {
			events.add( new KeyFieldEvent( first, start1, end1, second, start2, end2));
			return super.compareByteSequence(first, start1, end1, second, start2, end2, keyObj);
		}
			
	};
	static class DelegatedPartitioner extends KeyFieldBasedPartitioner<Text, Text> {
		static KeyFieldDemonstrator parent = null;
		
		@Override
		protected int hashCode(byte[] b, int start, int end, int currentHash) {
			events.add( new KeyFieldEvent( b, start, end));
			return super.hashCode( b, start, end, currentHash);    
		}
	}

	String keySpec;
	String partitionerSpec;
	

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected void customSetup(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		super.customSetup(conf);

		conf.setJobName("KeyFieldDemonstrator");
		conf.setNumReduceTasks(1);
		conf.setInt("io.sort.mb", 10);
		conf.setInputFormat( KeyValueTextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("mapred.job.tracker", "local");
		conf.set("fs.default.name", "file:///");
		conf.set("hadoop.tmp.dir","/tmp");
		File tmpIn = File.createTempFile("partition", ".in");
		File tmpOut = File.createTempFile("partition", ".out");
		if (!tmpOut.delete()) {
			throw new IOException( "Unable to delete file created for output");
		}
		PrintStream ps = new PrintStream(new FileOutputStream(tmpIn));

		ps.println( outputLine + "\t1");
		ps.println( outputLine + "\t2");
		ps.close();
		conf.setOutputKeyClass(Text.class);
		FileInputFormat.setInputPaths(conf, new Path( tmpIn.toString() ));
		FileOutputFormat.setOutputPath(conf, new Path( tmpOut.toString() ));

		
	
	}
	
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#handleRemainingArgs(org.apache.hadoop.mapred.JobConf, java.lang.String[])
	 */
	@Override
	protected boolean handleRemainingArgs(JobConf conf, String[] args) {
		if (args.length!=2) {
			System.err.println("a comparator key spec and a partitioner key space may be specified.");
			return false;
		}
		keySpec = args[0];
		partitionerSpec = args[1];
		conf.setKeyFieldComparatorOptions(keySpec);
		conf.setKeyFieldPartitionerOptions(partitionerSpec);
		conf.setOutputKeyComparatorClass(DelegatedComparator.class);
		conf.setPartitionerClass(DelegatedPartitioner.class);
		conf.set("map.output.key.field.separator", " ");
		
		events.add( new KeyFieldEvent( outputLine, keySpec, partitionerSpec));
		return true;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res;
		if (args.length==3) {
			res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(args[0]), new String[] { "-logLev", "ERROR", "-tsr", "FAILED", args[1], args[2] }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		} else {/** run a set of standard tests. */
		
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 1,1", "-k1,1" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 1,2", "-k1,2" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 1,3", "-k1,3" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 2,2", "-k2,2" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 2,3", "-k2,3" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 2,4", "-k2,4" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 2.1,2.2", "-k2.1,2.2" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 2.2,3.2", "-k2.2,3.2" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator(), new String[] { "-k 2.0,4.0", "-k2.0,4.0" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator("01234 6789 abc defg"), new String[] { "-k2.2,3.4r", "-k2.2,3.4r" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		res = ToolRunner.run(new Configuration(), new KeyFieldDemonstrator("01234 6789 abc"), new String[] { "-k1.2,2.3", "-k1.2,2.3" }); if (res!=0) {System.err.println("Job exit code is " + res);	}
		
		}
		for( KeyFieldEvent event : events) {
			System.out.println( event );
		}
		System.exit(res);
	}

}
