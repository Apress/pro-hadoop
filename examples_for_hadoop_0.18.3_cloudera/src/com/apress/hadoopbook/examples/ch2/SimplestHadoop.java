/** Define your package - standard java. Note this means that javac will place the class file in ./org/apress/examples/hadoop/ */
package com.apress.hadoopbook.examples.ch2;

/** The class that provides api for job configuration. */
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.log4j.Logger;

/**
 * A very simple class to provide a no-op hadoop map/reduce job.
 * 
 * It only accepts text as input, all of the input files are parsed, and sorted
 * then output. Only that portion of each input line up to but not including a
 * TAB character is used for sorting purposes. The defaults are to do all
 * processing locally and to use /tmp for input, output and temporary files.
 * This is superseeded in later examples which implement Tool and use the GenericOptionsParser
 * 
 * @author Jason Venner
 */

public class SimplestHadoop {
    /** The object to be used for logging. */
    private static Logger log = Logger.getLogger(SimplestHadoop.class);

    /** A very simple main method to run the default job.
     * The arguments are ignored.
     * If the job is not successful, the process with exit with a status of 1. If the job is successful the exit status will be 0.
     * 
     * @param args ignored.
     * @exception IOException if there are any lower level failures.
     */
    public static void main(String[] args) throws IOException {
        try {
            // Construct a JobConf object to use for running the job
            // Inform the framework that the it needs to make the jar that contains SimplestHadoop.class to all of the tasks.
            JobConf conf = new JobConf(SimplestHadoop.class);
            SimplestHadoop driver = new SimplestHadoop();
            driver.configureDefaults(conf);
            RunningJob runningJob = driver.run(conf);
            if (!runningJob.isSuccessful()) {
                log
                        .error("The job failed, please correct the underlying problem and try again.");
                System.exit(1);
            }
        } catch (Throwable e) {
            log.error("The job failed with an exception", e);
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new IOException("Unexpected exception", e);
        }

    }

    /**
     * The directory that reduce output will be written to. This directory must
     * not exist at job start time or the Hadoop framework will reject the job.
     * You can specify a full url for these paths, such as
     * <code>hdfs://namenode/user/jason/textInTextOut.output</code>. or
     * <code>file:///tmp/textInTextOut.output</code>. This directory must be
     * accessible by all MapReduce nodes via this url.
     */
    protected String outputPath = "textInTextOut.output";

    /**
     * The directory that job input will be read from. input has the same
     * requirements that {@link #outputPath} has.
     */
    protected String inputPath = "textInTextOut.input";
    /**
     * The directory to use as the base directory for the
     * 
     * @link {@link #inputPath} and for the {@link #outputPath). If this is
     *       unset, the Hadoop default base directory will be used.
     */
    protected String workingDirectory = "/tmp";

    /**
     * This is the default Jobtracker address. This is the server that the
     * hadoop jobs will be submitted to for execution. If this has the value of
     * <code>local</code>, the job will be run locally a single task at a
     * time, which is ideal for debugging. If the value is unset, the Hadoop
     * default Jobtracker will be used.
     */
    protected String jobTracker = "local";

    /**
     * The default number of map tasks. If this is a 0 or less, the Hadoop
     * default number will be used. For debugging set this to a low number,
     * other wise set it to some value greater than or equal to the number of
     * map execution spots in your cluster. Other factors can result in more or
     * fewer map tasks being created than this number.
     */

    protected int numMapTasks = 2;

    /**
     * The default number of reduces to run. For a single file of fully sorted
     * output set this to 1. For unsorted, unreduced output, set this to 0. if
     * the value is less than 0, the Hadoop framework default value will be
     * used. The number of reduces is always honored and you will only ever have
     * that many reduces run. Note: speculative execution can partially alter
     * the above statement.
     */
    protected int numReduceTasks = 1;

    /**
     * The shared file system url to use for the Hadoop frame work. This file
     * system has to be accessible by all of the Tasktracker nodes. If unset,
     * the Hadoop default file system will be used.
     */
    protected String fsDefaultName = "file:///";

    /**
     * The input format to use for this job. The default is
     * {@link KeyValueTextInputFormat} which reads text files a line at a time.
     * Each line is split into the key and value by the first TAB character.
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends FileInputFormat> inputFormat = KeyValueTextInputFormat.class;

    /**
     * The output format to use for this job. The default is
     * {@link TextOutputFormat} which produces one line of out put for each key
     * value pair, where the format is key TAB value <eol>
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends FileOutputFormat> outputFormat = TextOutputFormat.class;

    /**
     * The map output key class to use for this job. If unset the default is the
     * Hadoop framework default, Which will be
     * {@link JobConf#getOutputKeyClass()}.
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends WritableComparable> mapOutputKeyClass = null;

    /**
     * The map output value class to use for this job. If unset the default is
     * the Hadoop framework default, Which will be
     * {@link JobConf#getOutputValueClass()}.
     */
    protected Class<? extends Writable> mapOutputValueClass = null;

    /**
     * The output key class to use for this job. If unset the default is the
     * Hadoop framework default, Which will be
     * {@link JobConf#getOutputKeyClass()}, and seems to be by default
     * {@link org.apache.hadoop.io.LongWritable}.
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends WritableComparable> outputKeyClass = Text.class;

    /**
     * The output value class to use for this job. If unset the default is the
     * Hadoop framework default, Which will be
     * {@link JobConf#getOutputValueClass()}, and seems to be by default
     * {@link org.apache.hadoop.io.LongWritable}.
     */
    protected Class<? extends Writable> outputValueClass = Text.class;

    /**
     * The mapper class to use. Must be set. This class provides the map
     * function which is called for each input item. The default
     * {@link IdentityMapper} passes the input through unchanged.
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends Mapper> mapperClass = IdentityMapper.class;

    /**
     * The combiner class to use. This is a mini reduce for the output of a map
     * task. In general it may not be safe to set this to your reducer. The use
     * of this is a performance optimization that can minimize the amount of
     * data to be shuffled and sorted.
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends Reducer> combinerClass = null;

    /**
     * The parititioner class to use. This class is used to determine which
     * output/reduce partition each key is sent to before the reduce. The
     * framework default partitioner is
     * {@link org.apache.hadoop.mapred.lib.HashPartitioner HashPartitioner}. If
     * this value is null the Hadoop framework partitioner will be used. 
     * <br>Note: If you are using mapside joins, all input must have used the same Partitioner class.
     * @see org.apache.hadoop.mapred.join
     */
    @SuppressWarnings("unchecked")
	protected Class<? extends Partitioner> partitionerClass = null;

    /**
     * The recuer class to use. Must be set, if the
     * {@link #numReduceTasks number of reduces} is greater than zero This class
     * provides the map function which is called for each input item. The
     * default {@link IdentityReducer} passes writes one item of output for each
     * key, value pair.
     */
    @SuppressWarnings("unchecked")
    protected Class<? extends Reducer> reducerClass = IdentityReducer.class;

    public String getFsDefaultName() {
        return fsDefaultName;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends FileInputFormat> getInputFormat() {
        return inputFormat;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getJobTracker() {
        return jobTracker;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends WritableComparable> getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public Class<? extends Writable> getMapOutputValueClass() {
        return mapOutputValueClass;
    }

    public int getNumMapTasks() {
        return numMapTasks;
    }

    public int getNumReduceTasks() {
        return numReduceTasks;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends FileOutputFormat> getOutputFormat() {
        return outputFormat;
    }
    @SuppressWarnings("unchecked")
    public Class<? extends WritableComparable> getOutputKeyClass() {
        return outputKeyClass;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public Class<? extends Writable> getOutputValueClass() {
        return outputValueClass;
    }

    public String getWorkingDirectory() {
        return workingDirectory;
    }

    /** Actually launch the map reduce task, and return the results to the user.
     * 
     * The return object provides an {@link RunningJob#isSuccessful() isSuccessful} method, and provides access to the Job Counters.
     * 
     * @param conf The JobConf object that has been configured to provide the job parameters
     * @return The status of the job as a {@link RunningJob} object.
     * 
     * @throws IOException
     */
    public RunningJob run(JobConf conf) throws IOException {
        log.info("Starting job");
        RunningJob job = JobClient.runJob(conf);
        log.info("Job Finished");
        return job;
    }

    /**
     * Update the {@link JobConf} object with the defaults for this job.
     * 
     * Note: This method should be called before calling the {@link #run}
     * method of this object.
     * 
     * The parameters of the job can be set by calling the relevant setter
     * methods on this object, and then calling this method.
     * 
     * @param conf
     *            The JobConf object to initialize
     * @throws IOException
     *             If there is an IO error while working with the input or
     *             output path.
     */
    public void configureDefaults(JobConf conf) throws IOException {
        configureDefaultFileSystem(conf);

        configureWorkingDirectory(conf);

        configureInput(conf);
        
        configureOutput(conf);

        configureReduce(conf);
        
        configureMap(conf);


    }

    /** Configure the Map portion of the Hadoop job.
     * This method has the responsibility of configuring all of the map related parameters: <UL>
     * <li>Setting the number of Map Tasks, optional</li>
     * <li>Setting the {@link Mapper map} class, required.</li>
     * <li>Setting the {@link Reducer combiner} class, optional.</li>
     * <li>Informing the framework of the {@link JobConf#setMapOutputKeyClass map output key class}, optional. The default is the {@link JobConf#getOutputKeyClass output key class}.</li>
     * <li>Informing the framework of the {@link JobConf#setMapOutputValueClass map output value class}, optional, The default is the {@link JobConf#getOutputValueClass output value class}.</li>
     * </ul>
     * @param conf The {@link JobConf} object to configure.
     */
    private void configureMap(JobConf conf) {
        
        if (getNumMapTasks() > 0) {
            log.info("Setting the suggested number of map tasks to "
                     + getNumMapTasks());
            conf.setNumMapTasks(getNumMapTasks());
        } else {
            log.debug("The number of suggested map tasks is "
                    + conf.getNumMapTasks());
        }

        if (getMapperClass()==null) {
            throw new IllegalStateException("The mapperClass must be specified.");
        }
        log.info("Setting the mapper class to " + getMapperClass().getName());
        conf.setMapperClass(getMapperClass());

        if (getCombinerClass() != null) {
            log.info("Setting the map combiner class to "
                     + getCombinerClass().getName());
            conf.setCombinerClass(getCombinerClass());
        } // There is no default combiner class

        if (getMapOutputKeyClass() != null) {
            conf.setMapOutputKeyClass(getMapOutputKeyClass());
            log.info("Setting the map output key class to "
                     + getMapOutputKeyClass().getName());
        } // The default value may not be set yet, as it is not available until the output is configured.

        if (getMapOutputValueClass() != null) {
            conf.setMapOutputValueClass(getMapOutputValueClass());
            log.info("Setting the map output value class to "
                     + getMapOutputValueClass().getName());
        } // The default value may not be set yet, as it is not available until the output is configured.
    }

    /** Configure the Reduce portion of the Hadoop job.
     * This method has the responsibility of configuring all of the reduce related parameters: <UL>
     * <li>Setting the number of Reduce Tasks, optional.</li>
     * <li>Setting the {@link Reducer reduce} class, required if the number of reduce tasks is not zero.</li>
     * <li>Setting the {@link Partitioner partitioner} class, optional.</li> 
     * </ul>
     * @param conf The {@link JobConf} object to configure.
     * @throws IOException If the number of reduces specified is not 0 and the reduce class is unset.
     */
    private void configureReduce(JobConf conf) throws IOException {

        if (getNumReduceTasks() >= 0) {

            log.info("Setting the number of reduce tasks to " + getNumReduceTasks());
            conf.setNumReduceTasks(getNumReduceTasks());

        } else {

            log.debug("The number of reduce tasks is " + conf.getNumReduceTasks());

        }


        if (getNumReduceTasks()>0 && getReducerClass()==null) {

            throw new IOException( "The number of reduces is not zero (" + getNumReduceTasks() + ") and no reducer class is specified");
        }

        log.info("Setting the reducer class to " + getReducerClass().getName());
        conf.setReducerClass(getReducerClass());

        
        if (getPartitionerClass() != null) {

            log.info("Setting the partitioner class to " + getPartitionerClass().getName());
            conf.setPartitionerClass(getPartitionerClass());

        } else {

                log.debug("The partitioner class is " + conf.getPartitionerClass().getName());
        }

    }

    /**
     * @param conf
     * @throws IOException
     */
    private void configureOutput(JobConf conf) throws IOException {
        Path output = new Path(conf.getWorkingDirectory(), outputPath);
        FileSystem outputFileSystem = output.getFileSystem(conf);
        if (outputFileSystem.exists(output)) {
            throw new IOException(
                    "The specified output directory "
                            + output.makeQualified(outputFileSystem)
                            + " exists, please remove or specify an alternative output directory. Try hadoop dfs -rmr '"
                            + output.makeQualified(outputFileSystem) + "'");
        }

        log.info("Setting the output format to " + outputFormat.getName()
                + ", and the output Path to "
                + output.makeQualified(outputFileSystem));
        FileOutputFormat.setOutputPath(conf, output);
        conf.setOutputFormat(outputFormat);

        if (outputKeyClass != null) {
            conf.setOutputKeyClass(outputKeyClass);
            log.info("Setting the output key class to "
                    + outputKeyClass.getName());
        } else {
            log.info("The output key class is "
                    + conf.getOutputKeyClass().getName());
        }

        if (outputValueClass != null) {
            conf.setOutputValueClass(outputValueClass);
            log.info("Setting the output value class to "
                    + outputValueClass.getName());
        } else {
            log.info("The output value class is "
                    + conf.getOutputValueClass().getName());
        }

    }

    /**
     * @param conf
     * @throws IOException
     */
    private void configureInput(JobConf conf) throws IOException {
        {
        Path input = new Path(conf.getWorkingDirectory(), inputPath);
        FileSystem inputFileSystem = input.getFileSystem(conf);

        if (!inputFileSystem.exists(input)) {
            throw new IOException("The specified input directory "
                    + input.makeQualified(inputFileSystem) + " is missing");
        }

        log.info("Setting the input format to " + inputFormat.getName()
                + ", and the input Path to "
                + input.makeQualified(inputFileSystem));

        FileInputFormat.setInputPaths(conf, input);
        conf.setInputFormat(inputFormat);
        }
    }

    /**
     * @param conf
     */
    public void configureWorkingDirectory(JobConf conf) {
        if (workingDirectory != null) {
            log.info("setting the Hadoop default working directory to "
                    + workingDirectory);
            conf.setWorkingDirectory(new Path(workingDirectory));
        } else {
            log.debug("The default working directory is "
                    + conf.getWorkingDirectory());
        }
    }

    /**
     * @param conf
     */
    public void configureDefaultFileSystem(JobConf conf) {
        if (fsDefaultName != null) {
            log.info("setting the Hadoop default file system to "
                    + fsDefaultName);
            conf.set("fs.default.name", fsDefaultName);
        } else {
            log.debug("The default file system is "
                    + conf.get("fs.default.name"));
        }
    }

    public void setFsDefaultName(String fsDefaultName) {
        this.fsDefaultName = fsDefaultName;
    }

    @SuppressWarnings("unchecked")
    public void setInputFormat(Class<? extends FileInputFormat> inputFormat) {
        this.inputFormat = inputFormat;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public void setJobTracker(String jobTracker) {
        this.jobTracker = jobTracker;
    }

    @SuppressWarnings("unchecked")
    public void setMapOutputKeyClass(
            Class<? extends WritableComparable> mapOutputKeyClass) {
        this.mapOutputKeyClass = mapOutputKeyClass;
    }

    public void setMapOutputValueClass(
            Class<? extends Writable> mapOutputValueClass) {
        this.mapOutputValueClass = mapOutputValueClass;
    }

    public void setNumMapTasks(int numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    public void setNumReduceTasks(int numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }

    @SuppressWarnings("unchecked")
    public void setOutputFormat(Class<? extends FileOutputFormat> outputFormat) {
        this.outputFormat = outputFormat;
    }

    @SuppressWarnings("unchecked")
    public void setOutputKeyClass(
            Class<? extends WritableComparable> outputKeyClass) {
        this.outputKeyClass = outputKeyClass;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public void setOutputValueClass(Class<? extends Writable> outputValueClass) {
        this.outputValueClass = outputValueClass;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    @SuppressWarnings("unchecked")
    public void setMapperClass(Class<? extends Mapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Reducer> getCombinerClass() {
        return combinerClass;
    }

    @SuppressWarnings("unchecked")
    public void setCombinerClass(Class<? extends Reducer> combinerClass) {
        this.combinerClass = combinerClass;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Partitioner> getPartitionerClass() {
        return partitionerClass;
    }

    @SuppressWarnings("unchecked")
    public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) {
        this.partitionerClass = partitionerClass;
    }

    @SuppressWarnings("unchecked")
    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    @SuppressWarnings("unchecked")
    public void setReducerClass(Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
    }
 
}
