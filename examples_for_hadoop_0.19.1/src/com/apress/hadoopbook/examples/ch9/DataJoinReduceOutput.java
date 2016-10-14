package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.MainProgrameShell;


public class DataJoinReduceOutput extends MainProgrameShell {

	protected String[] inputs;
	protected String output;
	/** Given a set of directories, each of which contains part-XXXX files
	 * merge sort the output into a single file.
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected void customSetup(JobConf conf) throws IOException {
		// TODO Auto-generated method stub
		super.customSetup(conf);
		ArrayList<String> tables = new ArrayList<String>();
		for( String input : inputs ) {
			String []parts = input.split(":");
			if (parts.length==2) {
				Class<? extends InputFormat> candidateInputFormat = conf.getClass(parts[0],null,InputFormat.class);
				if (candidateInputFormat!=null) {
					addFiles(conf,candidateInputFormat, parts[1], tables);
					continue;
				}
			}
			addFiles(conf, KeyValueTextInputFormat.class,input, tables);
		}
		
		FileOutputFormat.setOutputPath(conf, new Path(output));
		conf.set("mapred.join.expr", "outer(" + StringUtils.join(tables, ",") + ")");
		conf.setNumReduceTasks(0);
		conf.setMapperClass(DataJoinMergeMapper.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(CompositeInputFormat.class);
		//conf.setOutputKeyComparatorClass(IPv4TextComparator.class);
		conf.setClass("mapred.join.keycomparator", IPv4TextComparator.class, WritableComparator.class);
		conf.setJarByClass(DataJoinMergeMapper.class);
		
	}

	
	@SuppressWarnings("unchecked")
	void addFiles(JobConf conf, Class<? extends InputFormat> inputFormat,	String path, ArrayList<String> tables) throws IOException {
		Path inputPath = new Path(path);
		FileSystem fs = inputPath.getFileSystem(conf);
		if (!fs.exists(inputPath)) {
			System.err.println(String.format("Input item %s does not exist, ignoring", path));
			return;
		}
		FileStatus status = fs.getFileStatus(inputPath);
		if (!status.isDir()) {
			String composed = CompositeInputFormat.compose(inputFormat, path); 
			if (verbose) { System.err.println( "Adding input " + composed); }
			tables.add(composed);
			return;
		}
		FileStatus[] statai = fs.listStatus(inputPath, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				if (path.getName().matches("^part-[0-9]+$")) {
					return true;
				}
				return false;
			}
		}
		);
		if (statai==null) {
			System.err.println(String.format("Input item %s does not contain any parts, ignoring", path));
			return;
		}
		for( FileStatus status1 : statai) {
			String composed = CompositeInputFormat.compose(inputFormat, status1.getPath().toString()); 
			if (verbose) { System.err.println( "Adding input " + composed); }
			tables.add(composed);
		}
	}


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
	
	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#handleRemainingArgs(org.apache.hadoop.mapred.JobConf, java.lang.String[])
	 */
	@Override
	protected boolean handleRemainingArgs(JobConf conf, String[] args) {
		// TODO Auto-generated method stub
		return super.handleRemainingArgs(conf, args);
	}

	/**
	 * @param args
	 */
	public static void main( String [] args ) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DataJoinReduceOutput(), args );
		System.exit(res);
	}


}
