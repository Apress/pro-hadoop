/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.MainProgrameShell;

/**
 * @author Jason
 *
 */
public class BruteForceMapReduceDriver extends MainProgrameShell {

	String[] inputs;
	String output;
	private Logger LOG = Logger.getLogger(BruteForceMapReduceDriver.class);
	
	
	@SuppressWarnings("static-access")
	@Override
	protected Options buildGeneralOptions(Options options) {
		options = super.buildGeneralOptions(options);
		options.addOption( OptionBuilder.withLongOpt("input")
				.withDescription("add an input path")
				.hasArgs()
				.isRequired()
			    .create("i") );
		options.addOption( OptionBuilder.withLongOpt("output")
				.withDescription("the output path")
				.hasArg()
				.isRequired()
			    .create("o") );
		return options;	
	}
	  
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected void customSetup(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		super.customSetup(conf);
		conf.setJobName("BruteForceRangeMapReduce");
		if (conf.getNumReduceTasks()!=1) {
			/** If more that one reduce is to be run, the spanning partitioner must be used. */
			conf.setClass("range.key.helper", PartitionedTextKeyHelperWithSeparators.class, KeyHelper.class);
		} else {
			/** Num reduces is 1, anything special? */
		}
		conf.setInt("io.sort.mb", 10);
		conf.setInputFormat( KeyValueTextInputFormat.class);
		for( String input : inputs) {
			if (verbose) {
				LOG.info("Adding input path " + input);
			}
			FileInputFormat.addInputPaths(conf, input);
		}
		if (verbose) {
			LOG.info( "Setting output path " + output);
		}
		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.setOutputFormat(TextOutputFormat.class);
		
		JobConf dummyConf = new JobConf(false);
		ChainMapper.addMapper(conf, ApacheLogTransformMapper.class,
				Text.class, Text.class, Text.class, Text.class,
				false,
				dummyConf);
		dummyConf.clear();
		ChainMapper.addMapper(conf, KeyValidatingMapper.class,
				Text.class, Text.class, Text.class, Text.class,
				false,
				dummyConf);

		dummyConf.clear();
		if (conf.getNumReduceTasks()>1) {
			/** Add in the map that takes incoming search space records and spans them across the partitions */
			ChainMapper.addMapper(conf, RangePartitionTransformingMapper.class,
					Text.class, Text.class, Text.class, Text.class, false, dummyConf);
			dummyConf.clear();
		}
		ChainReducer.setReducer(conf, ReducerForStandardComparator.class, Text.class, Text.class, Text.class, Text.class, false, dummyConf);
		dummyConf.clear();
		ChainReducer.addMapper(conf, TranslateBackToIPMapper.class, Text.class, Text.class, Text.class, Text.class, false, dummyConf);
	
	}

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#isSuccessFul(org.apache.hadoop.mapred.RunningJob)
	 */
	@Override
	protected int isSuccessFul(RunningJob rj) throws IOException {
		// TODO Auto-generated method stub
		return super.isSuccessFul(rj);
	}

	@Override
	protected void processGeneralOptions(JobConf conf, CommandLine commandLine) {
		super.processGeneralOptions(conf, commandLine);
		if (commandLine.hasOption('i')) {
			inputs = commandLine.getOptionValues('i');
		}
		if (commandLine.hasOption('o')) {
			output = commandLine.getOptionValue('o');
		}
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BruteForceMapReduceDriver(), args);
		if (res!=0) {
			System.err.println("Job exit code is " + res);
		}
		System.exit(res);
	}

}
