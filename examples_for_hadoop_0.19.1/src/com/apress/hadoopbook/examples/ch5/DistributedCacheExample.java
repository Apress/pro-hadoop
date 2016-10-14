/**
 * 
 */
package com.apress.hadoopbook.examples.ch5;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.ItemsToLookForInDistributedCacheData;
import com.apress.hadoopbook.utils.Utils;







/** A simple class demonstrating the DistributedCache.
 * 
 * 
 * @see org.apache.hadoop.util.GenericOptionsParser.
 * 
 * @author Jason
 * 
 */

public class DistributedCacheExample extends Configured implements Tool {

	public static Logger LOG = Logger.getLogger(DistributedCacheExample.class);
	
	public static String checkObjectConfigKey = "distributed.cache.example.check.object";

	

	/**
	 * 
	 */
	public DistributedCacheExample() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param conf
	 */
	public DistributedCacheExample(Configuration conf) {
		super(conf);
		// TODO Auto-generated constructor stub
	}


	

	/**
	 * Actually run the job, set up the configuration, force our mapper and
	 * disable reduce. Takes a an optional single argument --deleteOutput in
	 * which case if the ouput directory exists and the output format is text,
	 * the directory will be deleted before the job starts.
	 * 
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] args) throws Exception {
		if (args.length > 1
				|| (args.length == 1 && !"--deleteOutput".equals(args[0]))) {
			System.err
					.println("Usage: DistributedCacheExampe HadoopOptions [--deleteOutput]");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		boolean deleteOutput = args.length == 1;

		final JobConf job = new JobConf(getConf()); // Initialize the JobConf
													// object to be used for
													// this job from the command
													// line configured JobConf.
		/** Ensure that the jar file that contains DistributedCacheMapper is distributed to the tasks, and made available to the tasks. */
		job.setJarByClass(DistributedCacheMapper.class);
		
		/** This provides a class we can serialize into the Configuration with information about the items being passed
		 * in the distributed cache.
		 */
		ItemsToLookForInDistributedCacheData checkData = new ItemsToLookForInDistributedCacheData();
		
		/**
		 * Small memory use for small machines, my testing laptop has only 1gig
		 * of ram
		 */
		job.setInt("io.sort.mb", 10);
		/** Create 2 zip files for our distributed cache to contain. */
		/** This zipfile will be part of the class path */
		/** Note that the absolute path is provided. */
		Path jarFileForClassPath = Utils.makeAbsolute(Utils.setupArchiveFile(job, 10, true),job);
		DistributedCache.addArchiveToClassPath(jarFileForClassPath, job);
		checkData.addArchivesInClassPath(jarFileForClassPath, false);
		LOG.info( "Storing archive in classpath " + jarFileForClassPath + " in the distributed cache");
		
		/** This zip file will simply be available. */
		Path zipFileAsArchive = Utils.setupArchiveFile(job, 20, false);
		DistributedCache.addCacheArchive(zipFileAsArchive.toUri(), job);
		checkData.addArchivesNotInClassPath(zipFileAsArchive, false);
		LOG.info( "Storing archive " + zipFileAsArchive + " in the distributed cache");
		
		/** This zip file will be available in the working directory of the task. */
		Path zipFileForWorkingDirectory = Utils.setupArchiveFile( job, 30, false);
		URI zipFileForWorkingDirectoryWithSymLinkFragment = Utils.addSymLinkFragment(zipFileForWorkingDirectory);
		
		DistributedCache.addCacheArchive( zipFileForWorkingDirectoryWithSymLinkFragment, job );
		checkData.addArchivesNotInClassPath(zipFileForWorkingDirectory, true);
		LOG.info( "Storing archive " + zipFileForWorkingDirectoryWithSymLinkFragment + " in the distributed cache, with symlink");
		LOG.info( "zip File Path " + zipFileForWorkingDirectory.toUri());
		LOG.info( "zip File Path for symlinking to task working directory" + zipFileForWorkingDirectoryWithSymLinkFragment);
		
				
		/** Create a simple file that will be available. */
		Path simpleFileForCache = Utils.createSimpleTmpFile(job, true);
		DistributedCache.addCacheFile(simpleFileForCache.toUri(), job);
		checkData.addFilesNotInClassPath(simpleFileForCache, false);
		LOG.info( "Storing file " + simpleFileForCache + " in the distributed cache");
		
		/** Create a simple file that will be available in the current directory. */
		Path simpleFileForWorkingDirectory = Utils.createSimpleTmpFile(job, true);
		DistributedCache.addCacheFile(Utils.addSymLinkFragment(simpleFileForWorkingDirectory), job);
		checkData.addFilesNotInClassPath(simpleFileForWorkingDirectory, true);
		LOG.info( "Storing file " + simpleFileForWorkingDirectory + " in the distributed cache, with symlink");
		
		/** A simple file that will be in the class path. Note the passage of the absolute path */
		Path simpleFileForClassPath = Utils.makeAbsolute(Utils.createSimpleTmpFile(job, true),job);
		DistributedCache.addFileToClassPath(Utils.makeAbsolute(simpleFileForClassPath, job), job);
		checkData.addFilesInClassPath(simpleFileForClassPath, false);
		LOG.info( "Storing file in classpath " + simpleFileForClassPath + " in the distributed cache");
		
		/** Ensure that symbolic links are created. */
		DistributedCache.createSymlink(job);
		
		/** Save a copy of our check data object so we can verify in the mapper's config method. */
		ItemsToLookForInDistributedCacheData.set( job, checkObjectConfigKey, checkData);
		
		job.setJobName("distributedCacheJob");
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);

		job.setMapperClass(DistributedCacheMapper.class);
		job.setJarByClass(DistributedCacheMapper.class);
		/** Don't schedule any reduce jobs, we are just testing the configure method of the map class. */
		job.setNumReduceTasks(0);
		/**
		 * if the input format is textual and an input file has not been
		 * specified,make one with some random data.
		 */
		Utils.makeSampleInputIf(job, "distributedCacheInput", 2);
		/**
		 * If the output path is not set and it is text, set it to something
		 * reasonable.
		 */
		Utils.setupAndRemoveOutputIf(job, "distributedCacheOutput", deleteOutput);

		/** Send the job to the framework. */
		RunningJob rj = launch(job);
		Counters counters = rj.getCounters();
		System.out.println("The Job is "
				+ (rj.isComplete() ? " complete " : " incomplete")
				+ (rj.isComplete() ? " and "
						+ (rj.isSuccessful() ? " successfull " : " failed ")
						: ""));

		for (Counters.Group group : counters) {
			System.out.println("Counter Group: " + group.getDisplayName());
			for (Counters.Counter counter : group) {
				System.out.println("\t" + counter.getDisplayName() + "\t"
						+ counter.getCounter());
			}
		}
		return 0;
	}

	/**
	 * This is a stylized method that the Tool pattern requires.
	 * 
	 * @param job
	 *            The {@link JobConf} object to use to launch the job.
	 */
	protected RunningJob launch(JobConf job) throws IOException {
		JobConf conf = new JobConf(job, this.getClass());

		RunningJob rj = JobClient.runJob(conf);
		return rj;

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DistributedCacheExample(),
				args);
		System.exit(res);
	}

}
