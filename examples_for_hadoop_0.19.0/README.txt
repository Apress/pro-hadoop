This distribution is designed to be loaded into eclipse. It applicable hadoop source and jar files. The author used version eclipse 3.4.

If you are going to run under windows, you will need install cygwin, and have the cygwin bin directory in your windows path.

The hadoopprobook.jar file is a compiled jar of the book examples.


Source and jar files that are apache licensed are in the directory apache_licensed_lib.

Source and jar files that are bsd licensed are in the directory bsd_license.

Source and jar files that have other licenses are in the directory other_license.

The text of the applicable licenses are in these directories also.

The actual book examples are in the directory src/com/apress/hadoopbook/examples, ch2, ch5, ch7, ch8 and ch9, jobconf and advancedtechniques.

advancedtechniques contains a demonstration of static variable sharing via JVM reuse.

The support classes are in the directory src/com/apress/hadoopbook/

the directory src/config contains various files loaded as resources by the example classes.

The directory src/org/apache/hadoop/mapred/lib/ contains 2 source files that are modifications of 2 apache licensed files.

The original apache license applies. These classes have been modified to alow the enable richer examples of how to use the KeyFieldBasedComparator by providing less restricive access to members and methods

The unit tests are in test/src/com/apress/hadoopbook/ and test/src/com/apress/hadoopbook/examples/ ch7 and ch9. The directory test/src/org/apache/hadoop/examples contains the PiEstimatorTest case.

The .classpath and .project will be used by eclipse when it imports the project.

The file environment_variable_samples_for_launchers are sample values of environment variables to set in the environment that launches eclipse. These configure the runtime launching the examples via eclipse.

the launchers directory contains eclipse launch scripts for the examples, these are configured using the environment variables listed in environment_variable_samples_for_launchers.

access_log.txt and searchspace.txt contain the sample data for the chapter 9 example.



