package com.apress.hadoopbook.examples.ch5;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;

import com.apress.hadoopbook.utils.ExamplesMapReduceBase;
import com.apress.hadoopbook.utils.MainProgrameShell;
import com.apress.hadoopbook.utils.Utils;

/** Simple class to test the passing of shared libraries via the command line arguments
 * Run the job -file [PATH/]libbt.so where [PATH/]libbt.so exists and is a valid shared library.
 * [PATH/] is optional and may be a leading path.
 * This job will {@link #fakeLibraryName} which defaults to libbt.so in configure().
 * 
 * Supply a real shared library, it will be copied via the distributed cache and loaded in the
 * tasks.
 * @author Jason
 *
 */

public class SharedLibraryDemo extends MainProgrameShell {
	
	public static final String CONFIG_LIBRARY_NAME = "passed.library.name";
	private static final String CONSTRUCTED_LIBRARY_NAME = "constructed_library_name";
	/** The path to the shared library from the command line. */
	protected String library_path;
	/** The file name portion of the shared library. */
	protected String libraryName;
	
	@Override
	protected boolean handleRemainingArgs(JobConf conf, String[] args) {
		if (args.length != 1) {
			System.err.println("Only 1 argument, a shared library is accepted");
			return false;
		}
		library_path = args[0];
		File library = new File(library_path);
		if (!library.canRead()) {
			System.err.println("The supplied library must be readable: " + library_path );
			return false;
		}
		if (library.toString().matches("[0-9]$")) {
			System.err.println("Java loader will not accept versioned shared libraries such as " + library);
			return false;
		}
		libraryName = library.getName();
		return true;
	}


	protected static class SharedLibraryMapper extends ExamplesMapReduceBase
			implements Mapper<Text, Text, Text, Text> {
		String passedLibraryName;
		String constructedLibraryName;
		
		String loaderException = null;
		/** Load the shared library {@link #fakeLibraryName}.
		 * @see com.apress.hadoopbook.utils.ExamplesMapReduceBase#configure(org.apache.hadoop.mapred.JobConf)
		 */
		@Override
		public void configure(JobConf job) {
			super.configure(job);
			try {
				System.err.println( "java.library.path " + System.getProperty("java.library.path"));
				System.err.println("user.dir " + System.getProperty("user.dir"));
				File dot = new File(".");
				for( File file : dot.listFiles() ) {
					System.err.println(" File in . is " + file);
				}
				passedLibraryName = job.get(CONFIG_LIBRARY_NAME);
				constructedLibraryName = job.get(CONSTRUCTED_LIBRARY_NAME);
				String[] paths = System.getProperty("java.library.path").split(":");
				for( String path : paths) {
					File tmp = new File( path, passedLibraryName);
					if (tmp.exists()) {
						System.err.println("Found library at " + tmp);
						try {
							System.load(tmp.toString());
						} catch( Throwable ignore ) {
							System.err.println("Failed to load " + tmp);
						}
					}
				}

				/** This works sometimes */
				System.loadLibrary(constructedLibraryName);

			} catch( Exception e ) {
				loaderException = e.getMessage();
			}
		}

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			if (loaderException!=null) {
				reporter.incrCounter(passedLibraryName, loaderException, 1);
				loaderException=null;
			}
			output.collect(key,value);// identity
			
		}

	}
	

	/* (non-Javadoc)
	 * @see com.apress.hadoopbook.utils.MainProgrameShell#customSetup(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected void customSetup(JobConf conf) throws IOException {
		super.customSetup(conf);
		conf.set(CONFIG_LIBRARY_NAME, libraryName);
		String constructedLibraryName;
		if (System.getProperty("os.name").startsWith("Win")) {
			constructedLibraryName = libraryName.replaceFirst("\\.so(\\.[0-9.]+)?$", ""); /** Lose the .so stuff*/
		} else {
			constructedLibraryName = libraryName.replaceFirst("\\.so(\\.[0-9.]+)?$", ""); /** Lose the .so stuff*/
			constructedLibraryName = constructedLibraryName.replaceFirst("^lib", "");
		}
		conf.set(CONSTRUCTED_LIBRARY_NAME, constructedLibraryName);
		conf.setInputFormat(KeyValueTextInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		Utils.makeSampleInputIf(conf, "SharedLibraryDemo", 3);
		setDeleteOutputDir( true );
		FileOutputFormat.setOutputPath(conf,new Path("SharedLibraryDemo.out"));
		deleteOutputIf(conf);
		conf.setMapperClass(SharedLibraryMapper.class);
	}


	public static void main( String [] args ) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SharedLibraryDemo(), args);
		System.exit(res);
	}

}
