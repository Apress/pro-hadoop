package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.MainProgrameShell;

/**
 * The main class may be run as a map/reduce job to produce an output dataset suitable for use as a table in a mapside join, or
 * for inclusion in a brute force mapping, where the mapper is aware of RangeSpanning Keys.
 * It accepts data set arguments, the last argument is the output directory, all the prior are added as input paths
 * The reduce count must be set by defining it via -D mapred.reduce.tasks=N before the input file arguments start
 * 
 * @author Jason
 *
 */
public class IPRangePartitionerDriver extends MainProgrameShell {

	
	
	/** The input key class is defined as {@link Text} via {@link JobConf#setOutputKeyClass(Class)}.
	 * The input format is set to {@link KeyValueTextInputFormat}
	 * The Partitioner is {@link SimpleIPRangePartitioner} and the Mapper is {@link RangePartitionTransformingMapper}.
	 * 
	 * The number of reduces comes from -D mapred.reduce.tasks=N on the command line, default is 1
	 * 
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected void customSetup(JobConf conf) throws IOException {
		super.customSetup(conf);
		conf.setMapperClass(RangePartitionTransformingMapper.class);
		conf.setPartitionerClass(SimpleIPRangePartitioner.class);
		conf.setOutputKeyClass(Text.class);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setJarByClass(this.getClass());
	}
	
	

	/** any remaining arguments are the input files and the output file, in that order. There must be at least 2 arguments.
	 * 
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#handleRemainingArgs(org.apache.hadoop.mapred.JobConf, java.lang.String[])
	 */
	@Override
	protected boolean handleRemainingArgs(JobConf conf, String[] args) {
		if (args.length<2) {
			System.err.println( "Requires at least 2 arguments inputs and output");
			return false;
		}
		/** All arguments but the last one are considered input sources. */
		for( int i = 0; i < args.length-1; i++) {
			if (verbose) { System.out.println( "Adding input path " + args[i]); }
			FileInputFormat.addInputPath(conf,new Path(args[i]));
		}
		
		/** The last argument is the output. */
		if (verbose) { System.out.println( "Adding output path " + args[args.length-1]); }
		FileOutputFormat.setOutputPath(conf, new Path(args[args.length-1]));
		return true;
	}

	public static void main( String [] args ) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IPRangePartitionerDriver(), args );
		System.exit(res);
	}



}
