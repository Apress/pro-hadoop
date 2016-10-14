/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;
import java.util.Formatter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;

/** Perform simple validation on the keys.
 * 
 * @author Jason
 *
 */
 class TranslateBackToIPMapper extends ExamplesMapReduceBase
					implements Mapper<Text, Text, Text, Text> {
	
	 public static Logger LOG = Logger.getLogger(TranslateBackToIPMapper.class);

	 Text outputKey = new Text();

	 @Override
	 public void configure(JobConf job){
		 super.configure(job);
	 }

	 StringBuilder sb = new StringBuilder();
	 Formatter fmt = new Formatter(sb);
	 /** validate the key values, as being >= 0 and < 2^32 and if needed swap the begin and end range for search requests.
	  * 
	  * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	  */
	 @Override
	 public void map(Text key, Text value, OutputCollector<Text, Text> output,
			 Reporter reporter) throws IOException {
		 try {
			 reporter.incrCounter("TranslateBackToIPMapper", "TOTAL INPUT", 1);
			 String[] parts = key.toString().split("\t");
			 if (parts.length!=3) {
				 reporter.incrCounter("TranslateBackToIPMapper", "INVALID KEYS", 1);
				 return;
			 }
			 long address = Long.parseLong(parts[0],16);
			 long rangeBegin = Long.parseLong(parts[1],16);
			 long rangeEnd = Long.parseLong(parts[2],16);
			 sb.setLength(0);
			 unpack( address );
			 sb.append('\t');
			 unpack(rangeBegin);
			 sb.append('\t');
			 unpack(rangeEnd);
			 outputKey.set(sb.toString());
			 reporter.incrCounter("TranslateBackToIPMapper", "TOTAL OUTPUT", 1);
			 output.collect( outputKey, value );


		 } catch( Throwable e ) {
			 throwsIOExcepction(reporter, "TranslateBackToIPMapper", e);
		 }


	 }

	 protected void unpack(long address) {
		 long a = (address & 0xff000000) >> 24;
		 long b = (address & 0x00ff0000) >> 16;
		 long c = (address & 0x0000ff00) >> 8;
		 long d = (address & 0x000000ff);
		 fmt.format("%d.%d.%d.%d", a, b, c, d);
		 fmt.flush();
	 }

 }
