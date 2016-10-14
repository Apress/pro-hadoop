package com.apress.hadoopbook.examples.ch2;

import java.io.IOException;
import java.util.Formatter;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/** A simple class to handle the housekeeping for the MapReduceIntro example job.
 * 
 * 
 * <p>
 * This job explicitly configures the job to run, locally and without a
 * distributed file system, as a stand alone application.
 * </p>
 * <p>
 * The input is read from the directory /tmp/MapReduceIntroInput and the output is written to
 * the directory /tmp/MapReduceIntroOutput. If the directory /tmp/MapReduceIntroInput is missing or empty, it is created and
 * some input data files generated. If the directory /tmp/MapReduceIntroOutput is present, it is removed.
 * </p>
 * 
 * @author Jason Venner
 */
public class MapReduceIntroConfig {
    /**
     * Log4j is the recommended way to provide textual information to the user
     * about the job.
     */
    protected static Logger logger = Logger.getLogger(MapReduceIntroConfig.class);

    /** Some simple defaults for the job input and job output. */
    /**
     * This is the directory that the framework will look for input files in.
     * The search is recursive if the entry is a directory.
     */
    protected static Path inputDirectory = new Path("file:///tmp/MapReduceIntroInput");
    /**
     * This is the directory that the job output will be written to. It must not
     * exist at Job Submission time.
     */
    protected static Path outputDirectory = new Path("file:///tmp/MapReduceIntroOutput");

    /**
     * Ensure that there is some input in the <code>inputDirectory</code>,
     * the <code>outputDirectory</code> does not exist and that this job will
     * be run as a local stand alone application.
     * 
     * @param conf
     *            The {@link JobConf} object that is required for doing file
     *            system access.
     * @param inputDirectory
     *            The directory the input will reside in.
     * @param outputDirectory
     *            The directory that the output will reside in
     * @throws IOException
     */
    protected static void exampleHouseKeeping(final JobConf conf,
            final Path inputDirectory, final Path outputDirectory)
            throws IOException {
        /**
         * Ensure that this job will be run stand alone rather than relying on
         * the services of an external JobTracker.
         */
        conf.set("mapred.job.tracker", "local");
        /** Ensure that no global file system is required to run this job. */
        conf.set("fs.default.name", "file:///");
        /**
         * Reduce the in ram sort space, so that the user does not need to
         * increase the jvm memory size. This sets the sort space to 1 Mbyte,
         * which is very small for a real job.
         */
        conf.setInt("io.sort.mb", 1);
        /**
         * Generate some sample input if the <code>inputDirectory</code> is
         * empty or absent.
         */
        generateSampleInputIf(conf, inputDirectory);

        /**
         * Remove the file system item at <code>outputDirectory</code> if it
         * exists.
         */
        if (!removeIf(conf, outputDirectory)) {
            logger.error("Unable to remove " + outputDirectory + "job aborted");
            System.exit(1);
        }
    }

    /**
     * Generate <code>fileCount</code> files in the directory
     * <code>inputDirectory</code>, where the individual lines of the file
     * are a random integer TAB filename.
     * 
     * The file names will be file-N where is between 0 and
     * <code>fileCount</code> - 1. There will be between 1 and
     * <code>maxLines</code> + 1 lines in each file.
     * 
     * @param fs
     *            The file system that <code>inputDirectory</code> exists in.
     * @param inputDirectory
     *            The directory to create the files in. This directory must
     *            already exist.
     * @param fileCount
     *            The number of files to create.
     * @param maxLines
     *            The maximum number of lines to write to the file.
     * @throws IOException
     */
    protected static void generateRandomFiles(final FileSystem fs,
            final Path inputDirectory, final int fileCount, final int maxLines)
            throws IOException {

        final Random random = new Random();
        logger
                .info("Generating 3 input files of random data, each record is a random number TAB the input file name");

        for (int file = 0; file < fileCount; file++) {

            final Path outputFile = new Path(inputDirectory, "file-" + file);
            final String qualifiedOutputFile = outputFile.makeQualified(fs)
                    .toUri().toASCIIString();
            FSDataOutputStream out = null;
            try {
                /**
                 * This is the standard way to create a file using the Hadoop
                 * Framework. An error will be thrown if the file already
                 * exists.
                 */
                out = fs.create(outputFile);

                final Formatter fmt = new Formatter(out);
                final int lineCount = (int) (Math.abs(random.nextFloat())
                        * maxLines + 1);
                for (int line = 0; line < lineCount; line++) {
                    fmt.format("%d\t%s%n", Math.abs(random.nextInt()),
                            qualifiedOutputFile);
                }
                fmt.flush();
            } finally {
                /**
                 * It is very important to ensure that file descriptors are
                 * closed. The distributed file system code can run out of file
                 * descriptors and the errors generated in that case are
                 * misleading.
                 */
                out.close();
            }
        }
    }

    /**
     * This method will generate some sample input, if the
     * <code>inputDirectory</code> is missing or empty.
     * 
     * This method also demonstrates some of the basic API's for interacting
     * with file systems and files. Note: the code has no particular knowledge
     * of the type of file system.
     * 
     * @param conf
     *            The Job Configuration object, used for acquiring the
     *            {@link FileSystem} objects.
     * @param inputDirectory
     *            The directory to ensure has sample files.
     * @throws IOException
     */
    protected static void generateSampleInputIf(final JobConf conf,
            final Path inputDirectory) throws IOException {

        boolean inputDirectoryExists;
        final FileSystem fs = inputDirectory.getFileSystem(conf);

        if ((inputDirectoryExists = fs.exists(inputDirectory))
                && !isEmptyDirectory(fs, inputDirectory)) {
            if (logger.isDebugEnabled()) {
                logger
                        .debug("The inputDirectory "
                                + inputDirectory
                                + " exists and is either a file or a non empty directory");
            }
            return;
        }

        /**
         * We should only get here if <code>inputDirectory</code> does not
         * exist, or is an empty directory.
         */
        if (!inputDirectoryExists) {
            if (!fs.mkdirs(inputDirectory)) {
                logger.error("Unable to make the inputDirectory "
                        + inputDirectory.makeQualified(fs) + " aborting job");
                System.exit(1);
            }
        }
        final int fileCount = 3;
        final int maxLines = 100;
        generateRandomFiles(fs, inputDirectory, fileCount, maxLines);
    }

    /**
     * bean access getter to the {@link #inputDirectory} field.
     * 
     * @return the value of inputDirectory.
     */
    public static Path getInputDirectory() {
        return inputDirectory;
    }

    /**
     * bean access getter to the {@link #outputDirectory} field.
     * 
     * @return the value of outputDirectory.
     */
    public static Path getOutputDirectory() {
        return outputDirectory;
    }

    /**
     * Determine if a directory has any non zero files in it or it's descendant
     * directories.
     * 
     * @param fs
     *            The {@link FileSystem} object to use for access.
     * @param inputDirectory
     *            The root of the directory tree to search
     * @return true if the directory is missing or does not contain at least one
     *         none empty file.
     * @throws IOException
     */
    private static boolean isEmptyDirectory(final FileSystem fs,
            final Path inputDirectory) throws IOException {

        /**
         * This is the standard way to read a directory's contents. This can be
         * quite expensive for a large directory.
         */
        final FileStatus[] statai = fs.listStatus(inputDirectory);

        /**
         * This method returns null under some circumstances, in particular if
         * the directory does not exist.
         */
        if ((statai == null) || (statai.length == 0)) {
            if (logger.isDebugEnabled()) {
                logger.debug(inputDirectory.makeQualified(fs).toUri()
                        + " is empty or missing");
            }
            return true;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(inputDirectory.makeQualified(fs).toUri()
                    + " is not empty");
        }
        /** Try to find a file in the top level that is not empty. */
        for (final FileStatus status : statai) {
            if (!status.isDir() && (status.getLen() != 0)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("A non empty file "
                            + status.getPath().makeQualified(fs).toUri()
                            + " was found");
                    return false;
                }
            }
        }
        /** Recurse if there are sub directories, looking for a non empty file. */
        for (final FileStatus status : statai) {
            if (status.isDir() && isEmptyDirectory(fs, status.getPath())) {
                continue;
            }
            /**
             * If status is a directory it must not be empty or the previous
             * test block would have triggered.
             */
            if (status.isDir()) {
                return false;
            }
        }
        /**
         * Only get here if no non empty files were found in the entire subtree
         * of <code>inputPath</code>.
         */
        return true;
    }

    
    /**
     * Ensure that the <code>outputDirectory</code> does not exist.
     * 
     * <p>
     * The framework requires that the output directory not be present at job
     * submission time.
     * </p>
     * <p>
     * This method also demonstrates how to remove a directory using the
     * {@link FileSystem} api.
     * </p>
     * 
     * @param conf
     *            The configuration object. This is needed to know what file
     *            systems and file system plugins are being used.
     * @param outputDirectory
     *            The directory that must be removed if present.
     * @return true if the the <code>outputPath</code> is now missing, or
     *         false if the <code>outputPath</code> is present and was unable
     *         to be removed.
     * @throws IOException
     *             If there is an error loading or configuring the FileSystem
     *             plugin, or other IO error when attempting to access or remove
     *             the <code>outputDirectory</code>.
     */
    protected static boolean removeIf(final JobConf conf,
            final Path outputDirectory) throws IOException {

        /** This is standard way to acquire a FileSystem object. */
        final FileSystem fs = outputDirectory.getFileSystem(conf);

        /**
         * If the <code>outputDirectory</code> does not exist this method is
         * done.
         */
        if (!fs.exists(outputDirectory)) {
            if (logger.isDebugEnabled()) {
                logger
                        .debug("The output directory does not exists, no removal needed.");
            }
            return true;
        }
        /**
         * The getFileStatus command will throw an IOException if the path does
         * not exist.
         */
        final FileStatus status = fs.getFileStatus(outputDirectory);
        logger.info("The job output directory "
                + outputDirectory.makeQualified(fs) + " exists"
                + (status.isDir() ? " and is not a directory" : "")
                + " and will be removed");

        /**
         * Attempt to delete the file or directory. delete recursively just in
         * case <code>outputDirectory</code> is a directory with
         * sub-directories.
         */
        if (!fs.delete(outputDirectory, true)) {
            logger.error("Unable to delete the configured output directory "
                    + outputDirectory);
            return false;
        }

        /** The outputDirectory did exist, but has now been removed. */
        return true;

    }

    /**
     * bean access setter to the {@link #inputDirectory} field.
     * 
     * @param inputDirectory
     *            The value to set inputDirectory to.
     */
    public static void setInputDirectory(final Path inputDirectory) {
        MapReduceIntroConfig.inputDirectory = inputDirectory;
    }

    /**
     * bean access setter for the {@link #outputDirectory} field.
     * 
     * @param outputDirectory
     *            The value to set outputDirectory to.
     */
    public static void setOutputDirectory(final Path outputDirectory) {
        MapReduceIntroConfig.outputDirectory = outputDirectory;
    }

}
