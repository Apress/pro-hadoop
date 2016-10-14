/**
 * 
 */
package com.apress.hadoopbook.utils;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/** The file provides a base to write main hadoop program drivers.
 * The lib took and configure methods are handled.
 * @author Jason
 *
 */
public class MainProgrameShell extends Configured implements Tool {
	/** Make the application verbose. */
	protected boolean verbose = false;
	/** delete the output dir on start. */
	boolean deleteOutputDir = false;
	
	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return verbose;
	}

	/**
	 * @param verbose the verbose to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * @return the deleteOutputDir
	 */
	public boolean isDeleteOutputDir() {
		return deleteOutputDir;
	}

	/**
	 * @param deleteOutputDir the deleteOutputDir to set
	 */
	public void setDeleteOutputDir(boolean deleteOutputDir) {
		this.deleteOutputDir = deleteOutputDir;
	}

	/** general purpose logging. */
	static Logger LOG = Logger.getLogger(MainProgrameShell.class);


	/**
	 * 
	 */
	public MainProgrameShell() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public MainProgrameShell(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}
	/** Tool prescribes a run method that takes the unprocessed command line arguments and actually launches the job.
	 * 
	 * @return 0 on success, the returned value will be the exit code of the main method.
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	  public int run(String[] args) throws Exception {
		  final JobConf conf = new JobConf(getConf()); // Initialize the JobConf object to be used for this job from the command line configured JobConf.

		  int ret = runSetup(args, conf);
		  if (ret!=0) {
			  return ret;
		  }


		  /** Send the job to the framework. */
		  RunningJob rj = launch(conf);
		  outputJobCounters(rj);
		  return isSuccessFul(rj);
	  }

	/** All of the standard setup before launching a job. Put here to make run easier to override.
	 * 
	 * @param args The command line arguments left over by the {@link org.apache.hadoop.util.GenericOptionsParser GenericOptionsParser}.
	 * @param conf The JobConf object to configure
	 * @throws IOException
	 */
	protected int runSetup(String[] args, final JobConf conf) throws IOException {
		Options opts = buildGeneralOptions(null);
		  CommandLineParser parser = new GnuParser();
		  try {
			  CommandLine commandLine = parser.parse(opts, args, true);
			  processGeneralOptions(conf, commandLine);
			  args = commandLine.getArgs();
		  } catch(ParseException e) {
			  LOG .warn("options parsing failed: "+e.getMessage());

			  HelpFormatter formatter = new HelpFormatter();
			  formatter.printHelp("Custom options are: ", opts);
		  }
		  if (!handleRemainingArgs(conf, args)) {
			  HelpFormatter formatter = new HelpFormatter();
			  formatter.printHelp("Custom options are: ", opts);
			  ToolRunner.printGenericCommandUsage(System.err);
			  return -1;
		  }
		  customSetup(conf);

		  deleteOutputIf(conf);
		  return 0;
	}

	  /** This is where derived classes define required setup. */
	protected void customSetup(JobConf conf) throws IOException {
		
	}

	/** Delete the output directory if one is specified and it exists.
	 * 
	 * The member variable {@link #deleteOutputDir} indicates whether to delete or not.
	 * 
	 * @param conf THe configuration object to examine to determine if FileOutput is being used and the output path
	 * @throws IOException If the output directory exists and can not be deleted.
	 */
	protected void deleteOutputIf(final JobConf conf)
			throws IOException {
		if (deleteOutputDir&&conf.getOutputFormat() instanceof FileOutputFormat<?, ?>) {
			final Path output = FileOutputFormat.getOutputPath(conf);
			if (output==null) {
				LOG.error( "no output path specified for job");
				return;
			}
			final FileSystem fs = output.getFileSystem(conf);
			if (fs.exists(output)) {
				if (verbose) {
					LOG.info( "Deleting output path " + output);
				}
				if (!fs.delete(output,true)) {
					throw new IOException( "Unable to delete output path " + output);
				}
			}
		}
	}

	  /** Do something with any unprocessed command line arguments.
	   * 
	   * This base implementation returns false if there are any unhandled arguments
	   * 
	   * @param conf The job conf object to work with
	   * @param args	The left over arguments from the command line
	   * @return false on failure to trigger program exist
	   */
	  protected boolean handleRemainingArgs(JobConf conf, String[] args) {
		  if (args!=null&&args.length>0) {
			  LOG.error( "Unhandled arguments, " + args.length + " unhandled: "  + StringUtils.join( args, ", "));
			  return false;
		  }
		  return true;
	}

	/** Override this method to handle local arguments.
	   * 
	   * @param conf The job conf object to affect
	   * @param commandLine The command line to work from
	   */
	  protected void processGeneralOptions(JobConf conf, CommandLine commandLine) {
		if (commandLine.hasOption("v")) {
			verbose = true;
		}
		if (commandLine.hasOption("del")) {
			if (verbose) { LOG.info("Will Delete Output Directory If Present"); }
			deleteOutputDir = true;
		}
		if (commandLine.hasOption("logLev")) {
			final String logLevel = commandLine.getOptionValue("logLev");
			if (verbose) { LOG.info("Setting Log Level to " + logLevel); }
			LogManager.getRootLogger().setLevel(Level.toLevel(logLevel));
		}
		if (commandLine.hasOption("tsr")) {
			final String reporting = commandLine.getOptionValue("tsr");
			if (verbose) { LOG.info("Setting Client Output Filtering to " + reporting); }
			conf.set("jobclient.output.filter", reporting);
		}
		
	}

	  /** Override this to add additional argumetns.
	   * 
	   * @param options The options object to work with
	   * @return The options object
	   */
	@SuppressWarnings("static-access")
	protected Options buildGeneralOptions(Options options) {
		if (options==null) {
			options = new Options();
		}
		options.addOption( OptionBuilder.withLongOpt("verbose")
		.withDescription("make verbose")
	    .create("v") );
		
		options.addOption(  OptionBuilder.withLongOpt("deleteOutput")
		.withDescription("Delete the Output Directory if it exists")
		.create("del") );
		
		options.addOption( OptionBuilder.withLongOpt("logLevel")
				.hasArg()
				.withArgPattern("(?:DEBUG|INFO|WARN|ERROR)$", 0)
				.withDescription("Set the application log level.")
				.create("logLev") );
		
		options.addOption(
				OptionBuilder.withLongOpt("taskStatusReporting")
				.hasArg()
				.withArgPattern("^(?:NONE|KILLED|FAILED|SUCCEEDED|ALL)$^", 0)
				.withDescription("Set the level of task completion that will result in the task long being reported to this job's console. NONE|KILLED|FAILED|SUCCEEDED|ALL")
				.create("tsr")
				);
		
		return options;
	}

	/** Simple method to determine if the job succeeded. Ideally examine the counters to make that determination
	 * 
	 * @param rj
	 * @return
	 * @throws IOException
	 */
	protected int isSuccessFul(RunningJob rj) throws IOException
	{
		if (rj.isComplete()&&rj.isSuccessful()) {
			return 0;
		}
		return 1;
	}
	

	/** Dump the Job Counters to the Standard Output.
	 * 
	 * @param rj
	 * @throws IOException
	 */
	public static void outputJobCounters(RunningJob rj) throws IOException {
		Counters counters = rj.getCounters();
	    System.out.println( "The Job " + rj.getJobName() + " is " 
	    		+ (rj.isComplete() ? " complete " : " incomplete")
	    		+ (rj.isComplete() ? " and " + (rj.isSuccessful() ? " successfull " : " failed ") : "")
	    		);
	    		
	    for( Counters.Group group : counters) {
	    	System.out.println( "Counter Group: " + group.getDisplayName());
	    	for( Counters.Counter counter: group) {
	    		System.out.println( "\t" + counter.getDisplayName() + "\t" + counter.getCounter());
	    	}
	    }
	}
	  
	  /** This is a stylized method that the Tool pattern requires.
	   * 
	   * The job jar is explicitly set based on this class
	   * 
	   * @param passedInConf The {@link JobConf} object to use to launch the job.
	   */
	protected RunningJob launch(JobConf passedInConf) throws IOException {
		JobConf conf = new JobConf(passedInConf, this.getClass());
		
		RunningJob rj = JobClient.runJob(conf);
		return rj;
		
		
	}


//	/**
//	 * @param args
//	 * @throws Exception 
//	 */
//	public static void main(String[] args) throws Exception {
//		int res = ToolRunner.run(new Configuration(), new MainProgrameShell(), args);
//		System.exit(res);
//	}
//

}
