package com.apress.hadoopbook.examples.jobconf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;

/** Demonstrate how the final tag works for configuration files and is ignored
 *  by the {@link Configuration#set(java.lang.String,java.lang.String)} operator
 * This relies on the hadoop-core jar, and the hadoop-default.xml file being in the classpath.
 */
public class DemonstrationOfFinal {
	/** Save xml configuration data to a temporary file, that will be deleted on jvm exit
	 * 
	 * @param configData The data to save to the file
	 * @param baseName The base name to use for the file, may be null
	 * @return The File object for the file with the data written to it.
	 * @throws IOException
	 */
	static File saveTemporaryConfigFile( final String configData, String baseName ) throws IOException {
		if (baseName==null) {
			baseName = "temporaryConfig";
		}
		/** Make a temporary file using the JVM file utilities */
		File tmpFile = File.createTempFile(baseName, ".xml");
		tmpFile.deleteOnExit(); /** Ensure the file is deleted when this jvm exits. */
		Writer ow = null;
		/** Ensure that the output writer is closed even on errors. */
		try {
			ow = new OutputStreamWriter( new FileOutputStream( tmpFile ), "utf-8");
			ow.write( configData );
			ow.close();
			ow = null;
		} finally {
			if (ow!=null) {
				try {
					ow.close();
				} catch (IOException e) {
				// 	ignore, as we are already handling the real exception
				}
			}
		}
		return tmpFile;
		
	}
	public static void main( String [] args ) throws IOException {
		/** Get a local file system object, so that we can construct a local Path
		 * That will hold our demonstration configuration.
		 */

		File finalFirst = saveTemporaryConfigFile(
				"<?xml version=\"1.0\"?>\n" +
				"<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
				"<configuration>\n" +
				"	<property>\n" +
				"		<name>final.first</name>\n" +
				"		<value>first final value.</value>\n" +
				"       <final>true</final>\n" +
				"	</property>\n" +
				"</configuration>\n", 
				"finalFirst" );
		
		File finalSecond = saveTemporaryConfigFile(
				"<?xml version=\"1.0\"?>\n" +
				"<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n" +
				"<configuration>\n" +
				"	<property>\n" +
				"		<name>final.first</name>\n" +
				"		<value>This should not override the value of final.first.</value>\n" +
				"	</property>\n" +
				"</configuration>\n", 
				"finalSecond" );
		
		
		/** Construct a JobConf object with our configuration data. */
		JobConf conf = new JobConf( finalFirst.toURI().toString() );
		/** Add the additional file that will attempt to overwrite the final value of final.first. */
		conf.addResource( finalSecond.toURI().toString());
		System.out.println( "The final tag in the first file will prevent the final.first value in the second configuration file from inserting into the configuration" );
		System.out.println( "The value of final.first in the configuration is [" + conf.get("final.first") + "]" );
		/** Manually set the value of final.first to demonstrate it can be overridden. */
		conf.set("final.first", "This will override a final value, when applied by conf.set");
		System.out.println( "The value of final.first in the configuration is [" + conf.get("final.first") + "]" );
		
	}
}
