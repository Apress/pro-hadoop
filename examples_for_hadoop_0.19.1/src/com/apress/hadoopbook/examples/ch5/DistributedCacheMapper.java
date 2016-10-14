package com.apress.hadoopbook.examples.ch5;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;
import com.apress.hadoopbook.utils.ItemsToLookForInDistributedCacheData;
import com.apress.hadoopbook.utils.Utils;

/**
 * Sample Mapper shell showing various practices
 * 
 * K1 and V1 will be defined by the InputFormat. K2 and V2 will be the
 * {@link JobConf#getOutputKeyClass()} and
 * {@link JobConf#getOutputValueClass()}, which by default are LongWritable
 * and Text. K1 and V1 may be explicitly set via
 * {@link JobConf#setMapOutputKeyClass(Class)} and
 * {@link JobConf#setMapOutputValueClass(Class)}. If K1 and V1 are
 * explicitly set, they become the K1 and V1 for the Reducer.
 * 
 * @author Jason
 * 
 */
public class DistributedCacheMapper extends MapReduceBase
		implements Mapper<Text, Text, Text, Text> {
	  private static final Log LOG =
		    LogFactory.getLog(DistributedCacheMapper.class);
	

	/**
	 * Create a logging object or you will never know what happened in your
	 * task.
	 */

	/** This is passed via the JobConf from the job setup and is the list of items we are looking for
	 * in the distributed cache.
	 */
	ItemsToLookForInDistributedCacheData checkData;

	List<Path> classPathArchives;

	List<Path>  classPathFiles;
	/**
	 * Always save one of these away, they are so handy for almost any
	 * interaction with the framework.
	 */
	JobConf conf = null;
	boolean done = false;
	List<Path>  nonClassPathArchives;
	List<Path>  nonClassPathFiles;


	/** Used to prevent object churn in the map method. */
	Text outputKey = new Text();

	/** Used to prevent object churn in the map method. */
	Text outputValue = new Text();

	/** Take this early, it is handy to have. */
	TaskAttemptID taskId = null;
	/** Used in metrics reporting. */
	String taskName = null;
	/** Sample close method that sets the task status based on how many map exceptions there were.
	 * This assumes that the reporter object passed into the map method was saved and
	 *  that the JobConf object passed into the configure method was saved.
	 */
	public void close() throws IOException {
		super.close();
		LOG.info(taskId.isMap() ? "Map" : "Reduce" + " Task complete");
	}

	/** Method that demonstrates the DistributedCache.
	 * 
	 * This method assumes the class derives from {@link MapReduceBase} 
	 * and saves a copy of the JobConf object, the taskName and the taskId into member variables.
	 * 
	 * 
	 * If this method fails the Tasktracker will abort this task.
	 * @param job The Localized JobConf object for this task
	 */
	public void configure(JobConf job) {
		super.configure(job);
		LOG.info("Map Task Configure");
		this.conf = job;
		try {
			taskName = conf.getJobName();
			taskId = TaskAttemptID.forName(conf.get("mapred.task.id"));
			if (taskName == null || taskName.length() == 0) {
				/** if the job name is essentially unset make something up. */
				taskName = taskId.isMap() ? "map." : "reduce."
						+ this.getClass().getName();
			}
			
			try {
				checkData = ItemsToLookForInDistributedCacheData.get(job, DistributedCacheExample.checkObjectConfigKey);
				classPathArchives = ExamplesMapReduceBase.arrayToList(DistributedCache.getArchiveClassPaths(conf));
				nonClassPathArchives = ExamplesMapReduceBase.arrayToList(DistributedCache.getLocalCacheArchives(conf));
				classPathFiles = ExamplesMapReduceBase.arrayToList(DistributedCache.getFileClassPaths(conf));
				nonClassPathFiles = ExamplesMapReduceBase.arrayToList(DistributedCache.getLocalCacheFiles(conf));
				checkData.check(job);
			} catch (IOException e) {
				throw new RuntimeException( "Can't unpack check data from the conf", e);
			}
			
			
		} catch (RuntimeException e) {
			/** Try to dump the class path and the load path, and the contents of the cache directory if this failed. */
			StringBuilder logMsg = new StringBuilder();
			Formatter fmt = new Formatter(logMsg);
			fmt.format( "%s%n", "Task current working directory contents%n");
			fmt.format( "Task failed to initialize, classpath %s, library load path %s%n",
					System.getProperty("java.class.path"), System.getProperty("java.library.path"));
			
			File cwd = new File(".");
			File contents[] = cwd.listFiles();
			if (contents!=null) {
				for( File item : contents ) {
					fmt.format( "\t%s, a %s of size %d%n", item.getName(), item.isFile() ? "file" : "directory", item.length());
				}
			} else {
				fmt.format( "\t empty%n");
			}
			fmt.flush();
			LOG.error(logMsg,e);
			
			throw e;
		}
		LOG.info(taskId.isMap() ? "Map" : "Reduce" + " Task Configure complete");

	}


	/**
	 * The map function, this is a nested inner loop and should be as clean
	 * and simple as possible.
	 * 
	 * @param key
	 *            The key object for this record. The key contents are only
	 *            valid for the duration of the map method.
	 * @param value
	 *            THe value object for this record. The value contents are
	 *            only valid for the duration of the map method.
	 * @param output
	 *            The output collector for records that are to be output or
	 *            passed to the reducer.
	 * @param reporter
	 *            The object to report status back to the framework. This
	 *            also provides a 'I am not hung' heartbeat to the
	 *            framework.
	 * 
	 *            The framework reuses the key and value objects,
	 *            reinitializing the contents for each call to map. If the
	 *            map method is going to take more than the number of
	 *            milliseconds stored under the key mapred.task.timeout,
	 *            default 600000 (10 min) A call must be made on the
	 *            reporter object to heartbeat with the Tasktracker or the
	 *            task will be killed.
	 * 
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
	 *      java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
	 *      org.apache.hadoop.mapred.Reporter)
	 */
	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		
		/** Write our data to the output, and via the reporter object. */

		if (done) {
			return;
		}
		done = true;	/* only do this one time. */
		
		reportOnAllDistributedCacheItems(output, reporter);
		
		checkAllDistributedCacheItemsAgainstExpectedItems(output, reporter);
		
		checkSymLinkItems(output, reporter);
		
		checkClassPathItemLoads(output, reporter);

		outputKey.set("ClassPath");
		outputValue.set(System.getProperty("java.class.path"));
		output.collect(outputKey,outputValue);
		 
				
	}

	/** Verify that all items that were {@link DistributedCache#createSymlink(org.apache.hadoop.conf.Configuration)} exist in the current working directory.
	 * @param output The collector to write output to 
	 * @param reporter The reporter to report with.
	 * @throws IOException
	 */
	private void checkSymLinkItems(OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		/** Need to get the file system object for the current working directory. */
		FileSystem fs = new Path(".").getFileSystem(conf);
		
		/** Ensure that each item that is 'symlinked' is present in the current working directory.
		 * This will fail for mapred.job.tracker = local.
		 */
		for (String symlinkFileName : checkData.getSymLinkItems()) {
			outputKey.set("Symlink of " + symlinkFileName);
			if (!fs.exists(new Path(symlinkFileName))) {
				reporter.incrCounter("Missing Simlink Files", symlinkFileName, 1);
				outputValue.set("missing");
			} else {
				reporter.incrCounter("Found Simlink Files", symlinkFileName, 1);
				outputValue.set("found");
			}
			output.collect( outputKey, outputValue);
		}
	}

	/** Report on all of the items we expect in the {@link DistributedCache}
	 * Uses the data collected from {@link ItemsToLookForInDistributedCacheData}
	 * 
	 * @param output The output collector to use
	 * @param reporter The reporter to use
	 * @throws IOException
	 */
	private void reportOnAllDistributedCacheItems(
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		/** Sort through all of the items we are expecting to receive via the distributed cache
		 * and the items we actually received.
		 * Output the results via counter and to the job output.
		 */
		
		/** Make a map of all of the items from the distributed cache by category. */
		TreeMap<String,List<Path>> items = new TreeMap<String,List<Path>>();
		
		items.put( "ClassPath Archives", classPathArchives);
		items.put( "Non ClassPath Archives", nonClassPathArchives);
		items.put( "ClassPath Files", classPathFiles);
		items.put( "Non ClassPath Files", nonClassPathFiles);
		
		/** Log what we have by category. */
		for( Map.Entry<String, List<Path>> distributedCacheItem : items.entrySet()) {
			final String category = distributedCacheItem.getKey();
			final List<Path> cacheItems = distributedCacheItem.getValue();
			
			reporter.incrCounter("Distributed Cache", category, cacheItems.size());
			outputKey.set("Distributed Cache " + category);
			
			for (Path path : cacheItems) {
				outputValue.set(path.toString());
				output.collect(outputKey,outputValue);
			}
		}
	}

	/** Check the  {@link DistributedCache} items against the items we expect and report on the results.
	 * 
	 * @param output The output collector to use
	 * @param reporter The reporter to use
	 * @throws IOException
	 */
	private void checkAllDistributedCacheItemsAgainstExpectedItems(
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		/** Build the set of file names we are expecting by category. */
		TreeMap<String,ArrayList<String>> want = new TreeMap<String,ArrayList<String>>();
		want.put("ClassPath Archives", checkData.getArchivesInClassPath());
		want.put("Non ClassPath Archives", checkData.getArchivesNotInClassPath());
		want.put("ClassPath Files",checkData.getFilesInClassPath());
		want.put("Non ClassPath Files", checkData.getFilesNotInClassPath());
		
		/** Walk through the items we are expecting in the distributed cache by type,
		 * and report on what we find, including the actual path the item is stored at.
		 * Note that some items have different file names than you expected.
		 */
		for( Entry<String, ArrayList<String>> wantedItem : want.entrySet()) {
			final String category = wantedItem.getKey();
			final ArrayList<String> expectedFileNames = wantedItem.getValue();
			
			for (String fileName : expectedFileNames) {
				outputKey.set("Passed In " + fileName);
				Path foundPath = Utils.findItemInCache(fileName, conf);
				
				if (foundPath!=null) {
					outputValue.set(foundPath.toString());
					output.collect( outputKey, outputValue);
					reporter.incrCounter(category, fileName + ": " + foundPath.toString(), 1);
				} else {
					outputValue.set("Not Found");
					output.collect( outputKey, outputValue);
					reporter.incrCounter(category, fileName + ": missing", 1);
				}
			}
		}
	}

	/** Attempt to load the resources we expect to find in the class path, that were passed via the  {@link DistributedCache}.
	 * @param output The collector to write output to 
	 * @param reporter The reporter to report with.
	 * @throws IOException
	 */
	void checkClassPathItemLoads(OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		/** Go through the class path items and verify that we can actually find the resources
		 * that they define.
		 */
		ArrayList<String> allClassPathItems = new ArrayList<String>(checkData.getArchivesInClassPath());
		allClassPathItems.addAll(checkData.getFilesInClassPath());
		
		for (String classPathItem : allClassPathItems) {
			/** This could be a zip file or an individual file, so we create a list of items to get.
			 * In the case of an individual file the list will have 1 item.
			 * This is not efficient code, it is simple code.
			 */

			List<String> toLoad = null;
			if (classPathItem.endsWith(".jar")) {
				LOG.info("Loading items from " + classPathItem);
				toLoad = Utils.getZipEntries( Utils.findClassPathArchive(classPathItem,conf), conf);
			} else {
				toLoad = new ArrayList<String>(1);
				LOG.info("Loading item " + classPathItem);
				toLoad.add(classPathItem);
			}
			
			/** For each item we have to examine get the resource URI and try to load it. */
			for (String resourceName : toLoad) {
				final String tokenName = classPathItem + "." + resourceName;
				outputKey.set( "classpath resource " + tokenName );
				String resourceNameToLoad = "/" + resourceName;
				LOG.info( "Loading " + resourceNameToLoad + " from " + classPathItem);
				URL resourceURI = this.getClass().getResource(resourceNameToLoad); /** Need to have non relative resource names.*/
				if (resourceURI==null) {
					reporter.incrCounter( "No Resource URI from conf loader, " + resourceName + " trying context loader", tokenName, 1);
					resourceURI = Thread.currentThread().getContextClassLoader().getResource(resourceName);
					if (resourceURI==null) {
						reporter.incrCounter( "No Resource URI from context loader, trying alt name " + resourceNameToLoad + " for", tokenName, 1);
						resourceURI = Thread.currentThread().getContextClassLoader().getResource(resourceNameToLoad);
					}
					
				}
				if (resourceURI==null) {
					outputValue.set("No Resource Found");
					output.collect( outputKey, outputValue);
					reporter.incrCounter("No Resource Found", resourceName, 1);
					continue;
				}
				InputStream is = null;
				try {
					is = conf.getConfResourceAsInputStream(resourceName);
					if (is==null) {
						reporter.incrCounter("Missing ClassPath Items", tokenName, 1);
						outputValue.set("missing resource " + resourceURI);
					} else {
						reporter.incrCounter("Found ClassPath Items", tokenName, 1);
						outputValue.set("found resource " + resourceURI);
					}
					output.collect( outputKey, outputValue);
				} finally {
					if (is!=null) {
						try { is.close(); } catch( IOException ignore ) {}
					}
				}
			}
		}
	}
}
