/**
 * 
 */
package com.apress.hadoopbook.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Jason
 *
 */
public class SpringUtils {

	/**
	 * Initialize the spring environment. This is of course completely
	 * optional.
	 * 
	 * This method picks up the application context from a file, that is in
	 * the class path. If the file items are passed through the {@link org.apache.hadoop.filecache.DistributedCache DistributedCache} amd symlinked
	 * They will be in the class path.
	 * 
	 * @param conf The JobConf object to look for the spring config file names. If this is null, the default value is used.
	 * @param contextConfigName The token to look under in the config for the names
	 * @param defaultConfigString A default value
	 * @return TODO
	 * 
	 */
	public static ApplicationContext initSpring(JobConf conf, String contextConfigName,
			String defaultConfigString) {
		/**
		 * If you are a spring user, you would initialize your application
		 * context here.
		 */
		/** Lookup the context config files in the JobConf, provide a default value. */
		String applicationContextFileNameSet =
			conf == null ? defaultConfigString :
				conf.get( contextConfigName, defaultConfigString);
		Utils.LOG.info("Map Application Context File "
				+ applicationContextFileNameSet);
		
		/** If no config information was found, bail out. */
		if (applicationContextFileNameSet==null) {
			Utils.LOG.error( "Unable to initialize spring configuration using " + applicationContextFileNameSet );
			return null;
		}
		/** Attempt to split it into components using the config standard method of comma separators. */
		String[] components = StringUtils.split(applicationContextFileNameSet, ",");
	
		/** Load the Configuration. */
		ApplicationContext applicationContext = 
			new ClassPathXmlApplicationContext( components);
	
		return applicationContext;
	}

}
