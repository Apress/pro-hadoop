package com.apress.hadoopbook.examples.jobconf;


import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;

/** Simple class to demonstrate variable expansion within hadoop configuration values.
 * This relies on the hadoop-core jar, and the hadoop-default.xml file being in the classpath.
 */
public class VariableExpansion {
	public static void main( String [] args ) throws IOException {
		/** Get a local file system object, so that we can construct a local Path
		 * That will hold our demonstration configuration.
		 */

		/** The file variable-expansion-example.xml is in the src/config and bin/config directory of the eclipse project
		 * and supplied in the appendix. It must be placed either explicitly in the class path, or in a directory that is searched
		 * for this example to work.
		 */
		JobConf conf = new JobConf( VariableExpansion.class.getClassLoader().getResource("variable-expansion-example.xml").toString() );
		System.out.println( "The value of no.expansion is [" + conf.get("no.expansion") + "]" );
		System.out.println( "The value of expansion.from.configuration is [" + conf.get("expansion.from.configuration") + "]");
		System.out.println( "The value of expansion.from.JDK.properties is [" + conf.get("expansion.from.JDK.properties") + "]");
		System.out.println( "The value of java.io.tmpdir is [" + conf.get("java.io.tmpdir") + "]" );
		System.out.println( "The value of order.of.expansion is [" + conf.get("order.of.expansion") + "]" );
		System.out.println( "Nested variable expansion for nested.variable.expansion is [" + conf.get("nested.variable.expansion") +"]");
	}
}
