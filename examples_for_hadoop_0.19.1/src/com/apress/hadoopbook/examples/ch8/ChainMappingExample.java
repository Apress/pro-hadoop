/**
 * 
 */
package com.apress.hadoopbook.examples.ch8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.MainProgrameShell;
import com.apress.hadoopbook.utils.Utils;

/** Demonstrate basic {@link org.apache.hadoop.mapred.lib.ChainMapper} and {@link org.apache.hadoop.mapred.lib.ChainReducer}, and include output detailing when the chain operations take place.
 * 
 * @author Jason
 *
 */
public class ChainMappingExample extends MainProgrameShell {
	/** general purpose logging. */
	static Logger LOG = Logger.getLogger(ChainMappingExample.class);
	
	/** The configuration key used for providing a chain item with item specific configuration data. */
	static final String idLabel = "example.id.label";
	
	/** The specific configuration data for each map chain element. Passed via the configuration key {@link #idLabel}. */
	final String [] mapMapperIds = new String[] { "Master map", "map 1", "map 2" };
	/** The specific configuration data foreach reduce chain element. Passed via the configuration key {@link #idLabel}. */
	final String [] reduceMapperIds = new String[] { "reduce 1", "reduce 2" };
	/** The specific configuration data for the reduce. Passed via the configuration key {@link #idLabel}. */
	final String reduceId = "Master Reduce";
	

	/** Generate the input, and the chains for our job.
	 * For each element in {@link #mapMapperIds} a map chain element will be created,
	 * and the entry will be passed to that map under the configuration key {@link #idLabel}.
	 * 
	 * A reduce will be generated with the {@link #reduceId} passed via the configuration key {@link #idLabel}.
	 * For each element in {@link #reduceMapperIds} a map in the reduce chain will be created, and the entry will be passed
	 * to that map under the configuration key {@link #idLabel}.
	 * 
	 * The generated input directory will be ChainMappingExample and the output directory will be ChainMappingExample.out.
	 * This job is best run with the {@link org.apache.hadoop.mapred.JobTracker} set to local, via the <code>-jt local</code> command line argument.
	 * 
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected
	void customSetup(JobConf conf) throws IOException {
		super.customSetup(conf);


		Path root = new Path("ChainMappingExample");
		conf.setInputFormat(KeyValueTextInputFormat.class);
		Utils.makeSampleInputIf(conf, root.toString(), 5);
		FileInputFormat.setInputPaths(conf, root);
		FileOutputFormat.setOutputPath(conf, root.suffix(".output"));
		conf.setNumReduceTasks(1);
		conf.setInt("io.sort.mb", 10);
		
		JobConf dummyConf = new JobConf(false);
		for( String mapId : mapMapperIds ) {
			dummyConf.clear();
			dummyConf.set(idLabel, mapId);
			ChainMapper.addMapper(conf, ChainMappingExampleMapperReducer.class, Text.class, Text.class, Text.class, Text.class, false, dummyConf);
		}
		dummyConf.clear();
		dummyConf.set(idLabel, reduceId );
		ChainReducer.setReducer(conf, ChainMappingExampleMapperReducer.class, Text.class, Text.class, Text.class, Text.class, false, dummyConf);
		for( String mapId : reduceMapperIds ) {
			dummyConf.clear();
			dummyConf.set(idLabel, mapId);
			ChainReducer.addMapper(conf, ChainMappingExampleMapperReducer.class, Text.class, Text.class, Text.class, Text.class, false, dummyConf);
		}
		
	}
	


	/** If there were any exceptions the job did not succeed.
	 * 
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#isSuccessFul(org.apache.hadoop.mapred.RunningJob)
	 */
	protected int isSuccessFul(RunningJob rj) throws IOException
	{

		Counters counters = rj.getCounters();
	    long totalExceptions = 0;
		for( Counters.Group group : counters) {
	    	for( Counters.Counter counter: group) {
	    		if (counter.getDisplayName().equals("ReduceExceptionsTotal") ||
	    				counter.getDisplayName().equals("MapExceptionsTotal")) {
	    			totalExceptions  += counter.getCounter();
	    		}
	    	}
	    }
	    return (int) totalExceptions;
	}
		  


	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ChainMappingExample(), args);
		if (res!=0) {
			System.err.println("Job exit code is " + res);
		}
		System.exit(res);

	}

}
