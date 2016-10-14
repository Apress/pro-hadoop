package com.apress.hadoopbook.utils;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/** Class to record information about items placed in the distributed cache.
 * This class can be serialized to a string for storage in the JobConf.
 * 
 * It provides a {@link #check(JobConf) method} for checking the locations of items passed via the distributed cache.
 * 
 * @author Jason
 *
 */
public class ItemsToLookForInDistributedCacheData implements Serializable {
	transient static Logger LOG = Logger.getLogger(ItemsToLookForInDistributedCacheData.class);
	
	static final long serialVersionUID = 1L;

	
	/** Pack into the JobConf as serialized XML
	 * 
	 * @param conf The Configuration object to store in
	 * @param key The key to store under
	 * @param item The item to store
	 * @throws IOException
	 */
	public static void set( Configuration conf, String key, ItemsToLookForInDistributedCacheData item) throws IOException {
		conf.set( key, Utils.serializeToXML(item));
	}
	/** Unpack from the job conf.
	 * 
	 * @param conf The Configuration object to unpack from
	 * @param key The key to lookup
	 * @return The object
	 * @throws IOException on error, or if the wrong type is unpacked
	 */
	public static ItemsToLookForInDistributedCacheData get( Configuration conf, String key) throws IOException
	{
		String raw = conf.get(key);
		if (raw==null) {
			return null;
		}
		Serializable item = Utils.deSerializeFromXML(raw);
		if (item instanceof ItemsToLookForInDistributedCacheData) {
			return (ItemsToLookForInDistributedCacheData) item;
		}
		throw new IOException( "De Serialized Item is of type "
				+ item.getClass().getName() + " not " + ItemsToLookForInDistributedCacheData.class.getName());
	}

	/** Return the list of class path archives that were added. */
	public ArrayList<String> getArchivesInClassPath() {
		return archivesInClassPath;
	}
	/** Return the list of non class path archives that were added. 
	 * These commonly have their name changed in hadoop version through at least 0.19.
	 */
	public ArrayList<String> getArchivesNotInClassPath() {
		return archivesNotInClassPath;
	}

	/** Return the list of classpath files added. */
	public ArrayList<String> getFilesInClassPath() {
		return filesInClassPath;
	}

	/** Return the list of files added that are not in the class path.
	 * These commonly have their name changed in hadoop at least through 0.19
	 * @return The list, which may be empty
	 */
	public ArrayList<String> getFilesNotInClassPath() {
		return filesNotInClassPath;
	}

	/** Return the list of items that were marked as to be symlinked. */
	public ArrayList<String> getSymLinkItems() {
		return symLinkItems;
	}

	/** The list of file names of archives that should be in the class path. */
	public ArrayList<String> archivesInClassPath = new ArrayList<String>();
	/** the list of file names of archives that should be present but not in the classpath. */
	public ArrayList<String> archivesNotInClassPath = new ArrayList<String>();
	/** The list of file names of files the should be in the class path. */
	public ArrayList<String> filesInClassPath = new ArrayList<String>();
	/** The list of file names of files that should not be in the class path. */
	public ArrayList<String> filesNotInClassPath = new ArrayList<String>();
	/** The list of names that should be symlinked. */
	public ArrayList<String> symLinkItems = new ArrayList<String>();
	
	

	public void addArchivesInClassPath(Path item, boolean symLink) {
		this.archivesInClassPath.add(item.getName());
		if(symLink) {
			addSymLinkItems(item);
		}
	}

	public void addArchivesNotInClassPath(Path item, boolean symLink) {
		archivesNotInClassPath.add(item.getName());
		if(symLink) {
			addSymLinkItems(item);
		}
	}
	
	public void addArchivesNotInClassPath(String itemString, boolean symLink) {
		Path item = new Path(itemString);
		archivesNotInClassPath.add(item.getName());
		if(symLink) {
			addSymLinkItems(item);
		}
	}

	public void addFilesInClassPath(Path item, boolean symLink) {
		filesInClassPath.add(item.getName());
		if(symLink) {
			addSymLinkItems(item);
		}
	}

	public void addFilesNotInClassPath(Path item, boolean symLink) {
		this.filesNotInClassPath.add(item.getName());
		if(symLink) {
			addSymLinkItems(item);
		}
	}

	public void addSymLinkItems(Path item) {
		symLinkItems.add(item.getName());
	}

	public ItemsToLookForInDistributedCacheData(
			ArrayList<String> archivesInClassPath,
			ArrayList<String> archivesNotInClassPath, ArrayList<String> filesInClassPath,
			ArrayList<String> filesNotInClassPath, ArrayList<String> symLinkItems) {
		this.archivesInClassPath = archivesInClassPath;
		this.archivesNotInClassPath = archivesNotInClassPath;
		this.filesInClassPath = filesInClassPath;
		this.filesNotInClassPath = filesNotInClassPath;
		this.symLinkItems = symLinkItems;
	}

	public ItemsToLookForInDistributedCacheData() {
		
	}
	static class NamePathMap extends HashMap<String,Path> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public NamePathMap( final Path[] input ) {
			if (input==null) {
				return;
			}
			for (Path entry : input) {
				put( entry.getName(), entry);
				LOG.info( "Found cache entry " + entry);
			}
		}
		/** Do a safe lookup of items that should be in the distributed cache and return the actual file name.
		 * The {@link DistributedCache} at least through hadoop 0.19.0 has a bug or feature in 
		 * the way it constructs relative path names. The code does not put a Path.SEPARATOR between the component it adds and the file name.
		 * 
		 * @param key The key to lookup, should be the trailing path component originally added to the DistributedCache
		 * @param conf The JobConf object
		 * @return either the actual trailing path component in the DistributedCache or null if not found.
		 */
		public String containsKey(final String key, final Configuration conf) {
			if(super.containsKey(key)) {
				return key;
			}
			String keyRelative = Utils.makeRelativeName( key, conf);
			if (keyRelative==null) {
				return null;
			}
			if (super.containsKey(keyRelative)) {
				return keyRelative;
			}
			return null;
		}
	}
	
	public void check(JobConf conf) throws IOException
	{

		
		LOG.info("archive class paths");
		NamePathMap dcClasspathArchives = new NamePathMap(DistributedCache.getArchiveClassPaths(conf));
		LOG.info("archive local paths");
		NamePathMap dcNonClasspathArchives = new NamePathMap(DistributedCache.getLocalCacheArchives(conf));
		LOG.info("file class paths");
		NamePathMap dcClasspathFiles = new NamePathMap(DistributedCache.getFileClassPaths(conf));
		LOG.info("file local paths");
		NamePathMap dcNonClasspathFiles = new NamePathMap(DistributedCache.getLocalCacheFiles(conf));
		
		
		LOG.info( "Have classpath archives " +StringUtils.join( archivesInClassPath, ", "));
		LOG.info( "Want classpath archives " +StringUtils.join( dcClasspathArchives.values(), ", "));
		for ( String archive : archivesInClassPath) {
			LOG.info("Checking class path archive " + archive);
			if (dcClasspathArchives.containsKey(archive)) {
				continue;
			}
			LOG.error("Unable to find classpath archive " + archive);
		}
			
		LOG.info( "Have non classpath archives " +StringUtils.join( archivesNotInClassPath, ", "));
		LOG.info( "Want non classpath archives " +StringUtils.join( dcNonClasspathArchives.values(), ", "));

		for ( String archive : archivesNotInClassPath) {
			LOG.info("Checking non class path archive " + archive);
			String foundPath;
			if ((foundPath = dcNonClasspathArchives.containsKey(archive,conf))!=null) {
				if (!foundPath.equals(archive)) {
					LOG.info( "Non cache archive " + archive + " became " + foundPath + " " + dcNonClasspathArchives.get(foundPath));
				}
				continue;
			}
			LOG.error("Unable to find non classpath archive " + archive);
		}
		
		LOG.info( "Have classpath files " +StringUtils.join( filesInClassPath, ", "));
		LOG.info( "Want classpath files " +StringUtils.join( dcClasspathFiles.values(), ", "));
		for ( String file : filesInClassPath) {
			LOG.info("Checking class path file " + file);
			if (dcClasspathFiles.containsKey(file)) {
				continue;
			}
			LOG.error("Unable to find classpath file " + file);
		
		}
		
		LOG.info( "Have non classpath files " +StringUtils.join( filesNotInClassPath, ", "));
		LOG.info( "Want non classpath files " +StringUtils.join( dcNonClasspathFiles.values(), ", "));
		for ( String file : filesNotInClassPath) {
			LOG.info("Checking non class path file " + file);
			String foundPath;
			if ((foundPath = dcNonClasspathFiles.containsKey(file,conf))!=null) {
				LOG.info( "Non cache file " + file + " became " + foundPath + " " + dcNonClasspathFiles.get(foundPath));
				continue;
			}
			LOG.error("Unable to find non classpath file " + file);
		}
		FileSystem fs = FileSystem.getLocal(conf);
		LOG.info( "Have symlink items " +StringUtils.join( symLinkItems, ", "));
		Path wd = new Path(".");
		Path[] paths = FileUtil.stat2Paths(wd.getFileSystem(conf).listStatus(new Path(".")));
		LOG.info( "Working directory has " + StringUtils.join( paths, ", "));
		for ( String name : symLinkItems) {
			LOG.info("Checking symlink item " + name);
			if (!fs.exists(new Path(name))) {
				LOG.error( "name " + name + " is not present in the working directory");
			}
		
		}
	}
	public void setArchivesInClassPath(ArrayList<String> archivesInClassPath) {
		this.archivesInClassPath = archivesInClassPath;
	}
	public void setArchivesNotInClassPath(ArrayList<String> archivesNotInClassPath) {
		this.archivesNotInClassPath = archivesNotInClassPath;
	}
	public void setFilesInClassPath(ArrayList<String> filesInClassPath) {
		this.filesInClassPath = filesInClassPath;
	}
	public void setFilesNotInClassPath(ArrayList<String> filesNotInClassPath) {
		this.filesNotInClassPath = filesNotInClassPath;
	}
	public void setSymLinkItems(ArrayList<String> symLinkItems) {
		this.symLinkItems = symLinkItems;
	}	
	
}