/**
 * 
 */
package com.apress.hadoopbook.utils;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueLineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/** This class exists to support the TotalOrderPartitioning example. An input format was needed
 * that handled long keys as input and output.
 * 
 * @author Jason
 *
 */
public class LongLongTextInputFormat extends FileInputFormat<LongWritable, LongWritable> implements
		InputFormat<LongWritable, LongWritable> {

	/** The actual KeyValueLineRecordReader. */
	RecordReader<Text,Text> realReader;
	/** Provide a delegate reader that takes the text values and makes them long writables
	 * 
	 * @author Jason
	 *
	 */
	class DelegateReader implements RecordReader<LongWritable,LongWritable> {
		Text key = realReader.createKey();
		Text value= realReader.createValue();
		/**
		 * @throws IOException
		 * @see org.apache.hadoop.mapred.RecordReader#close()
		 */
		public void close() throws IOException {
			realReader.close();
		}
		/**
		 * @return
		 * @see org.apache.hadoop.mapred.RecordReader#createKey()
		 */
		public LongWritable createKey() {
			return new LongWritable();
		}
		/**
		 * @return
		 * @see org.apache.hadoop.mapred.RecordReader#createValue()
		 */
		public LongWritable createValue() {
			return new LongWritable();
		}
		/**
		 * @return
		 * @throws IOException
		 * @see org.apache.hadoop.mapred.RecordReader#getPos()
		 */
		public long getPos() throws IOException {
			return realReader.getPos();
		}
		/**
		 * @return
		 * @throws IOException
		 * @see org.apache.hadoop.mapred.RecordReader#getProgress()
		 */
		public float getProgress() throws IOException {
			return realReader.getProgress();
		}
		/** Delegated next, read the textual values from the the data source and convert them into LongWritables.
		 * 
		 * @param key The key object to fill with the next record's key
		 * @param value The value object to fill with the next records value
		 * @return true if a record was read or false if at EOF
		 * @throws IOException
		 * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
		 */
		public boolean next(LongWritable key, LongWritable value) throws IOException {
			/** Perform the real read. */
			final boolean res = realReader.next(this.key, this.value);
			if (!res) { /** If at eof, we are done. */
				return false;
			}
			/** Attempt to convert the two text values read into LongWritables.
			 * If there is an error, throw an IOException.
			 */
			try {
				key.set(Long.valueOf(this.key.toString()));
				value.set(Long.valueOf(this.value.toString()));
				return true;
			} catch( NumberFormatException e) {
				throw new IOException("Invalid key, value " + key + ", " + value);
			}
		}

	}
		
	

	@Override
	public RecordReader<LongWritable, LongWritable> getRecordReader(InputSplit split, JobConf job,
			Reporter reporter) throws IOException {
		reporter.setStatus(split.toString());
	    realReader = new KeyValueLineRecordReader(job, (FileSplit) split);
		
		return new DelegateReader();
	}

}
