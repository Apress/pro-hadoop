package com.apress.hadoopbook.examples.ch2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;

/** A very simple MapReduce example that reads textual input where each record is a single line, and sorts all of the input lines into a single output file.
 * 
 * The records are parsed into Key and Value using the first TAB character as a separator. If there is no TAB character the entire line is the Key.
 * 
 * 
 * @author Jason Venner
 *
 */
public class MapReduceIntro {
    protected static Logger logger = Logger.getLogger(MapReduceIntro.class);

    /**
     * Configure and run the MapReduceIntro job.
     * 
     * @param args
     *            Not used.
     */
    public static void main(final String[] args) {
        try {

            /**
             * Construct the job conf object that will be used to submit this
             * job to the Hadoop framework. ensure that the jar or directory
             * that contains MapReduceIntroConfig.class is made available to all of the
             * Tasktracker nodes that will run maps or reduces for this job.
             */
            final JobConf conf = new JobConf(MapReduceIntro.class);
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

            /** Inform the framework that the mapper class will be the {@link IdentityMapper}.
             * This class simply passes the input Key Value pairs directly to it's output, which in our case will be the shuffle.
             */
            conf.setMapperClass(IdentityMapper.class);

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
            conf.setOutputKeyClass(Text.class);
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
            logger.info("The job completed successfully.");
            System.exit(0);
        } catch (final IOException e) {
            logger.error("The job has failed due to an IO Exception", e);
            e.printStackTrace();
        }

    }
}
