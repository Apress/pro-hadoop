/**
 * 
 */
package com.apress.hadoopbook.examples.ch9;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;

/** For the brute force log file processing example pass through keys that are already in the correct form
 * and for others attempt to parse them as lines that start with an IP address in one of the many standard forms.
 * and transform these into search request style key value pairs.
 * 
 * The key helper defaults to {@link TextKeyHelperWithSeparators} and the configuration key <code>range.key.helper</code> is used
 * to lookup the key helper.
 * 
 * @author Jason
 *
 */
public class ApacheLogTransformMapper extends
		ExamplesMapReduceBase implements Mapper<Text, Text, Text, Text> {
	
	protected static Logger LOG = Logger.getLogger(ApacheLogTransformMapper.class);
	
	Text outputKey = new Text();
	Text outputValue = new Text();
	
	/** Used for parsing log file lines. */
	StringBuilder sb = new StringBuilder();
	
		/** The key helper, this could be shared by all maps in the chain if we put them all in the same file
	 * For clarity of code each is in a separate file. There are some interesting optimizations
	 * that could be done if they shared variables.
	 */
	TextKeyHelperWithSeparators helper;
	
	@Override
	public void configure(JobConf conf) {
		super.configure(conf);
		helper = ReflectionUtils.newInstance(conf.getClass("range.key.helper", TextKeyHelperWithSeparators.class,
                TextKeyHelperWithSeparators.class),conf); 
			
		
	}

	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		try {
			
			reporter.incrCounter("ApacheLogTransformMapper", "TOTAL INPUT", 1);
			
			if (helper.getFromRaw(key)) {
				reporter.incrCounter("ApacheLogTransformMapper", "ALREADY PREPARED KEYS", 1);
				if (LOG.isDebugEnabled()) { LOG.debug("complete key passed forward untouched [" + key + "]"); }
				output.collect( key, value );
				return;
			}
			if (LOG.isDebugEnabled()) { LOG.debug("Working on [" + key + "]"); }
			reporter.incrCounter("ApacheLogTransformMapper", "LOG LINES", 1);
			String logLine = key.toString();
			String keyValueSeparator = conf.get("key.value.separator.in.input.line", "\t");
			String ipAddress;
			/** The IP address in the standard log file entry is the first field, with a trailing space to separate
			 * it from the next field.
			 */
			if (keyValueSeparator.length()==1 && keyValueSeparator.charAt(0)==' ') {
				/** The key and value are already parsed out. */
				ipAddress = logLine;
				if (parseAddressIntoKey(ipAddress, outputKey, reporter)) {
					reporter.incrCounter("ApacheLogTransformMapper", "VALID LOG LINES", 1);	
					if (LOG.isDebugEnabled()) { LOG.debug( "Key transforms from [" + key + "] to [" + outputKey + "]"); }
					output.collect(outputKey, value);
					return;
				}
			} else {
				/** For paranoia sake, re-assemble the log line and split it ourselves on the first space. */
				sb.setLength(0);
				sb.append(logLine);
				sb.append( keyValueSeparator);
				sb.append( value.toString());
				logLine = sb.toString();
				
				int indexOfSpace = logLine.indexOf(' ');
				if (indexOfSpace< 7 || indexOfSpace > 15) { /** xxx.xxx.xxx.xxx = 15 chars, 1.1.1.1 = 7 chars */
					if (LOG.isDebugEnabled()) { LOG.debug("Log line does not start with an ip address [" + logLine + "]" ); }
					reporter.incrCounter("ApacheLogTransformMapper", "BAD LOG LINES", 1);
					return;
				}
				
				ipAddress = logLine.substring(0,indexOfSpace);
				logLine = logLine.substring(indexOfSpace+1);
				
				if (parseAddressIntoKey(ipAddress, outputKey, reporter)) {
					outputValue.set( logLine );
					reporter.incrCounter("ApacheLogTransformMapper", "VALID LOG LINES", 1);
					if (LOG.isDebugEnabled()) { LOG.debug( "Key transforms from [" + key + "] to [" + outputKey + "]"); }
					output.collect( outputKey, outputValue );
					return;
				}
			}
		} catch( Throwable e ) {
			throwsIOExcepction(reporter, "map for key" + key + " failed", e);
		}
		
	}

	/** Take an IP address in some format and convert it to an unsigned 32 bit int, and store it in <code>outputKey</code>
	 * @param ipAddress The string to look in
	 * @param outputKey The {@link Text} object to store the result in
	 * @param reporter The {@link Reporter} object to use to report failures.
	 * @return true if the address was converted successfully.
	 */
	protected boolean parseAddressIntoKey( final String ipAddress, final Text outputKey, final Reporter reporter) {
		long address = AbstractKeyHelper.addrToLong(ipAddress);
		if (address<0 || (address&0xffffffff)!=0) {
			reporter.incrCounter("ApacheLogTransformMapper", "Bad IPv4 ADDRESS", 1);
		}
		helper.setSearchRequest(address);
		helper.setToRaw(outputKey);
		return helper.isValid();
	}
}