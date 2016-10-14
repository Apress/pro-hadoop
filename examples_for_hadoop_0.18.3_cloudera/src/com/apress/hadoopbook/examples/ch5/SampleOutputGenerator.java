package com.apress.hadoopbook.examples.ch5;

import java.io.IOException;
import java.util.Formatter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.apress.hadoopbook.utils.GenerateDataFile;

/** Generate our input data of record id and random positive integer.
 * 
 * @author Jason
 *
 */
public class SampleOutputGenerator extends GenerateDataFile {

	final Path path;
	final FileSystem fs;
	int which;
	Random random = new Random();

	public SampleOutputGenerator( final Configuration conf, final Path fileName ) throws IOException
	{
		this.path = fileName;
		this.fs = this.path.getFileSystem(conf);
	}
	/** Set the id number for the output file. */
	public void setWhich(int i) {
		which = i;
	}
	/** Generate a file name based on {@link SampleOutputGenerator#path} and {@link SampleOutputGenerator#which}.
	 * 
	 * @see com.apress.hadoopbook.utils.GenerateDataFile#getFileName(org.apache.hadoop.conf.Configuration, int, int)
	 */
	@Override
	public Path getFileName(Configuration conf, int start, int count) {
		return path.suffix("." +which);
	}

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.GenerateDataFile#outputLine(org.apache.hadoop.conf.Configuration, java.util.Formatter, int, int, int, int, org.apache.hadoop.fs.Path, org.apache.hadoop.fs.FSDataOutputStream)
	 */
	@Override
	public void outputLine(Configuration conf, Formatter fmt, int ordinal,
			int seq, int start, int end, Path filename,
			FSDataOutputStream out) {
		fmt.format("%d\t%d%n", seq, random.nextInt()& Integer.MAX_VALUE);
	}
}