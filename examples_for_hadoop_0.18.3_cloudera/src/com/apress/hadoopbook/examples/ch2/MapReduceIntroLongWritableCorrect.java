package com.apress.hadoopbook.examples.ch2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;

/** A modification of MapReduceIntro.java that uses a custom mapper to produce LongWritable values to ensure numerically sorted output
 * 
 * The records are parsed into Key and Value using the first TAB character as a separator. If there is no TAB character the entire line is the Key.
 * 
 * 
 * @author Jason Venner
 *
 */
public class MapReduceIntroLongWritableCorrect {
    protected static Logger logger = Logger.getLogger(MapReduceIntroLongWritableCorrect.class);

    /**
     * Configure and run the MapReduceIntroLongWritable job.
     * 
     * @param args
     *            Not used.
     */
    public static void main(final String[] args) {
        try {

            /**
             * Construct the job conf object that will be used to submit this
             * job to the Hadoop framework. ensure that the jar or directory
             * that contains MapReduceIntroLongWritableConfig.class is made available to all of the
             * Tasktracker nodes that will run maps or reduces for this job.
             */
            final JobConf conf = new JobConf(MapReduceIntroLongWritableCorrect.class);
            conf.set("hadoop.tmp.dir","/tmp");

            /**
             * Take care of some housekeeping to ensure that this simple example
             * job will run
             */
            MapReduceIntroConfig.exampleHouseKeeping(conf, MapReduceIntroConfig.getInputDirectory(), MapReduceIntroConfig.getOutputDirectory());

            /**
             * This section is the actual job configuration portion /**
             * Configure the inputDirectory and the type of input. In this case
             * we are stating that the input is text, and each record is a
             * single line, and the first TAB is the separator between the key
             * and the value of the record.
             */
            conf.setInputFormat(KeyValueTextInputFormat.class);
            FileInputFormat.setInputPaths(conf, MapReduceIntroConfig.getInputDirectory());

            /** Inform the framework that the mapper class will be the {@link TransformKeysToLongMapper}.
             * This the map method of this class transforms the Text,Text keys, value pairs into LongWritable,Text key value pairs.
             *
             */
            conf.setMapperClass(TransformKeysToLongMapper.class);

            /** Configure the output of the job to go to the output directory.
             * Inform the framework that the Output Key and Value classes will be {@link Text} and the output file format will
             * {@link TextOutputFormat}. The TextOutput format class joins produces a record of output for each Key,Value pair, with the following format.
             * Formatter.format( "%s\t%s%n", key.toString(), value.toString() );.
             *
             * In additon indicate to the framework that there will be
             * 1 reduce. This results in all input keys being placed
             * into the same, single, partition, and the final output
             * being a single sorted file.
             */
            FileOutputFormat.setOutputPath(conf, MapReduceIntroConfig.getOutputDirectory());
            // This looks like a good change, but it doesn't work, as the KeyValueTextInput is producing Text object keys.
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Text.class);
            conf.setNumReduceTasks(1);
            
            /** Inform the framework that the reducer class will be the {@link IdentityReducer}.
             * This class simply writes an output record key, value record for each value in the key, valueset it recieves as input.
             * The value ordering is arbitrary.
             */
            conf.setReducerClass(IdentityReducer.class);

            logger .info("Launching the job.");
            /** Send the job configuration to the framework and request that the job be run. */
            final RunningJob job = JobClient.runJob(conf);
            logger.info("The job has completed.");

            if (!job.isSuccessful()) {
                logger.error("The job failed.");
                System.exit(1);
            }
			/** Get the job counters. {@see RunningJob.getCounters()}. */
			Counters jobCounters = job.getCounters();

			/** Lookup the "Input" Group of counters. */
			Counters.Group inputGroup = jobCounters.getGroup( TransformKeysToLongMapper.INPUT );

			/** The map task potentially outputs 4 count counters in the input group. Get each of them. */
			long total = inputGroup.getCounter( TransformKeysToLongMapper.TOTAL_RECORDS );
			long parsed = inputGroup.getCounter( TransformKeysToLongMapper.PARSED_RECORDS );
			long format = inputGroup.getCounter( TransformKeysToLongMapper.NUMBER_FORMAT );
			long exceptions = inputGroup.getCounter( TransformKeysToLongMapper.EXCEPTION );
			
			if (format != 0) {
				logger.warn( "There were " + format + " keys that were not transformable to long values");
			}
			
			/** Check to see if we had any unexpected exceptions, this usually indicates some significant problem,
			 * either with the machine running the task that had the exception, or the map or reduce function code.
			 * Log an error for each type of exception with the count.
			 */
			if (exceptions > 0 ) {
				Counters.Group exceptionGroup = jobCounters.getGroup( TransformKeysToLongMapper.EXCEPTIONS );
				for (Counters.Counter counter : exceptionGroup) {
					logger.error( "There were " + counter.getCounter() + " exceptions of type " + counter.getDisplayName() );
				}
			}
			
			if (total == parsed) {
				logger.info("The job completed successfully.");
				System.exit(0);
			}
			
			// We had some failures in handling the input records. Did enough records process for this to be a successful job?
			// is 90% good enough?
			if (total * .9 <= parsed) {
				logger.warn( "The job completed with some errors, " + (total - parsed) + " out of " + total );
				System.exit( 0 );
			}
			
			logger.error( "The job did not complete successfully, to many errors processing the input, only " + parsed + " of " + total + "records completed" );
			System.exit( 1 );
        } catch (final IOException e) {
            logger.error("The job has failed due to an IO Exception", e);
            e.printStackTrace();
        }

    }
}
