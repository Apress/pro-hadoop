
package com.apress.hadoopbook.examples.ch2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/** Transform the input Text, Text key value pairs into LongWritable, Text key value pairs. */
public class TransformKeysToLongMapper
    extends MapReduceBase implements Mapper<Text, Text, LongWritable, Text> {
    public static final String EXCEPTIONS = "Exceptions";
	public static final String EXCEPTION = "Exception";
	public static final String NUMBER_FORMAT = "number format";
	public static final String INPUT = "Input";
	public static final String PARSED_RECORDS = "parsed records";
	public static final String TOTAL_RECORDS = "total records";
	/** Provide a way for log messages to be managed. */
  private Logger logger = Logger.getLogger( TransformKeysToLongMapper.class);

  /** map input to the output, transforming the input {@link Text} keys into {@link LongWritable} keys.
   * The values are passed through unchanged.
   *
   * Report on the status of the job.
   * @param key The input key, supplied by the framework, a {@link Text} value.
   * @param value The input value, supplied by the framework, a {@link Text} value.
   * @param output The {@link OutputCollector} that takes {@link LongWritable}, {@link Text} pairs.
   * @param reporter The object that provides a way to report status back to the framework.
   * @exception IOException if there is any error.
   */
  public void map(Text key, Text value,
                  OutputCollector<LongWritable, Text> output, Reporter reporter)
    throws IOException {

      try {
          try {
              reporter.incrCounter( TransformKeysToLongMapper.INPUT, TransformKeysToLongMapper.TOTAL_RECORDS, 1 );
              LongWritable newKey =  new LongWritable( Long.parseLong( key.toString() ) );
              reporter.incrCounter( TransformKeysToLongMapper.INPUT, TransformKeysToLongMapper.PARSED_RECORDS, 1 );
              output.collect(newKey, value);
          } catch( NumberFormatException e ) {
              /** This is a somewhat expected case and we handling it specially. */
              logger.warn( "Unable to parse key as a long for key, value " + key + " " + value, e );
              reporter.incrCounter( TransformKeysToLongMapper.INPUT, TransformKeysToLongMapper.NUMBER_FORMAT, 1 );
              return;
          }
      } catch( Throwable e ) {
          /** It is very important to report back if there were exceptions in the mapper.
           * In particular it is very handy to report the number of exceptions.
           * If this is done, the driver can make better assumptions on the success or failure of the job.
           */
          logger.error( "Unexpected exception in mapper for key, value " + key + ", " + value, e );
          reporter.incrCounter( TransformKeysToLongMapper.INPUT, TransformKeysToLongMapper.EXCEPTION, 1 );
          reporter.incrCounter( TransformKeysToLongMapper.EXCEPTIONS, e.getClass().getName(), 1 );
          if (e instanceof IOException) {
              throw (IOException) e;
          }
          if (e instanceof RuntimeException) {
              throw (RuntimeException) e;
          }
          throw new IOException( "Unknown Exception", e );
      }
  }
}
