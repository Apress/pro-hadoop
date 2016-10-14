package com.apress.hadoopbook.utils;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.log4j.Logger;


/** Simple Container for utility functions.
 * 
 * @author Jason
 *
 */
public class Utils {


	/** Logging is good. */
	static Logger LOG = Logger.getLogger(Utils.class);
	
	/** If the job conf is using a file input format and no input path has been specified, make one
	 * and setup the job to use it.
	 * @param job The JobConf object to work with
	 * @param name The name of the default file to make
	 * @param lineCount The number of lines in the file
	 * @throws IOException
	 */
	public static void makeSampleInputIf(final JobConf job, String name, int lineCount) throws IOException {
		@SuppressWarnings("unused")
		InputFormat<?,?> inputFormat;
		if ((inputFormat = job.getInputFormat()) instanceof FileInputFormat) {
			Path []inputPaths = FileInputFormat.getInputPaths(job);
			if (inputPaths==null||inputPaths.length==0) {
				Path input = new Path( name );
				FileInputFormat.setInputPaths(job, input);
				/** Has the input format been set? */
				if (job.get("mapred.input.format.class")==null) {
					job.setInputFormat(KeyValueTextInputFormat.class);
				}
				FileSystem fs = input.getFileSystem(job);
				if (!fs.exists(input)) {
					FSDataOutputStream out = null;
					try {
						out = fs.create(input);
											
						Formatter fmt = new Formatter(out);
						for (int lineNo = 0; lineNo < lineCount; lineNo++) {
							fmt.format("Key%d\tValue%d%n", lineNo, lineNo);
						}
						fmt.flush();
					} finally {
						if (out!=null) {
							out.close();
						}
					}
				}
			}
		}
	}
	
		
	/** Simple tool to serialize an object into an XML string so it can be passed in vai the JobConf object.
	 * 
	 * The representation is XML so it is not compact.
	 * @param toSerialize The object to serialize
	 * @return The string representation as utf-8 xml
	 * 
	 * @throws IOException
	 */
	public static String serializeToXML( Serializable toSerialize) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
		XMLEncoder oos = new XMLEncoder(baos);
		oos.writeObject(toSerialize);
		oos.close();
		return baos.toString("UTF-8");
	}
	
	/** De-Serialize an object that was packed with {@link #serializeToXML(Serializable)}.
	 * 
	 * @param xml The xml block
	 * @return the object
	 * 
	 * @throws IOException
	 */
	public static Serializable deSerializeFromXML( String xml ) throws IOException
	{
		byte[] raw = xml.getBytes("UTF-8");
		ByteArrayInputStream bais = new ByteArrayInputStream(raw);
		XMLDecoder decoder = new XMLDecoder(bais);
		return (Serializable) decoder.readObject();
	}
	
	/** Create a directory of single record files
	 * 
	 * The file names are tmpFile-XXXX where XXXX is the ordinal number of the file, the width of the ordinal numbers is 0 padded and even in the set of constructed filenames.
	 * Each file will be given one line of data, path TAB ordinalNumber EOL
	 * 
	 * @param conf The configuration file to use
	 * @param root The path to the directory. Missing intermediate directories will be made if there is work to do.
	 * @param count The number of files to create. If <= 0 no work will be done
	 * @param existingEntries A list to add the created paths to, if null a new list is created.
	 * @return null on failure to create the directory, or the list of created files
	 * @throws IOException If there is an error.
	 */
	
	public static List<Path> createSimpleDirectory( final Configuration conf, final Path root, final int count, List<Path> existingEntries ) throws IOException
	{
		/** If count is invalid just return, no work to do here. */
		if (count<=0) {
			LOG.error( "Count is <= 0 " + count + " for root " + root);
			return existingEntries;
		}
		/** If the incoming list is null create one that will cound at least count elements. */
		if (existingEntries==null) {
			existingEntries = new ArrayList<Path>(count);
		}
		FileSystem fs = root.getFileSystem(conf);
		if (fs.exists(root)&&!fs.getFileStatus(root).isDir()) {
			LOG.error("Root " + root + " exists and is not a directory");
			return null;
		}
		if (!fs.exists(root)&&!fs.mkdirs(root)) {
			LOG.error("Root " + root + " does not exist and can not be made");
			return null;
		}
		
		/** Setup a formatter for building the file names. */
		StringBuilder sb = new StringBuilder();
		Formatter fmt = new Formatter(sb);

		/** How wide is count, so we can set up the file name format nice and evenly.
		 * We first format the max numeric value into a string.
		 * Then determine the string width.
		 * This is now the field with to format for our output counters.
		 * Then we build a format string that has the width embedded in it, with a leading 0 for zero padding.
		 */
	 
		fmt.format("%d", count-1);
		fmt.flush();
		/** The width of the largest numeric suffix. */
		int countWidth = sb.toString().length();
		sb.setLength(0);
		fmt.format("tmpFile-%%0%dd", countWidth);
		/** The format string to use, with fixed width numeric suffixes. */
		String fmtString = sb.toString();
		/** Create the temporary files in root, with a name of the form tmpFile-0XXXX */
		for( int i = 0; i < count; i++) {
			/** Set up the file name in sb. */
			sb.setLength(0);
			fmt.format( fmtString, i);
			fmt.flush();
			String fileName = sb.toString();
			
			/** Create the file and create some data for it. */
			Path fileToCreate = new Path( root, fileName);
			sb.setLength(0);
			fmt.format( "%s\t%d%n", fileToCreate.toString(), count);
			fmt.flush();
			
			createSimpleFile( conf, fileToCreate, sb.toString());	/** Create a simple file with a single entry, the path and the count */
			existingEntries.add(fileToCreate);
		}
		return existingEntries;
	}

	/** Create a file with contents and return.
	 * 
	 * @param conf The Configuration object to use to work out how to access the file system of file
	 * @param file The path to create
	 * @param data The data to write to file.
	 * 
	 * @throws IOException
	 */
	public static void createSimpleFile( final Configuration conf, final Path file, String data ) throws IOException
	{
		FSDataOutputStream out = null;
		try {
			out = file.getFileSystem(conf).create(file);
			out.write(data.getBytes("UTF-8"));
		} finally {
			if (out!=null) {
				out.close();
			}
		}
	}
	
	/** Given a path return the path component as an absolute path from the root.
	 * Remove any protocol and scheme portions
	 * @param path The path to make absolute
	 * @param conf The Configuration to use
	 * @return The resulting absolute path
	 * @throws IOException
	 */
	public static Path makeAbsolute( Path path, final Configuration conf) throws IOException
	{
		FileSystem fs = path.getFileSystem(conf);
		if (!path.isAbsolute()) {
			path = new Path(fs.getWorkingDirectory(), path);
		}
		
		URI pathURI = path.makeQualified(fs).toUri();
		return new Path(pathURI.getPath());
		
	}
	
	public static Path stageFileToHdfsIf( JobConf conf, Path path, boolean markForDelete ) throws IOException
	{
		
		FileSystem pathFs = path.getFileSystem(conf);
		/** If the path doesn't exist bail out. */
		if (!pathFs.exists(path)) {
			return null;
		}
		/** If the file system uri's are the same, then the file systems are the same.
		 * This will get a few things wrong, if the port on one is specifed and the default port == the specified port. 
		 */
		FileSystem defaultFs = new Path(".").getFileSystem(conf);
		/** Do in a block to keep dummy and defaultFs out of the function local variable name space. */
			
		if (defaultFs.getUri().normalize().equals(pathFs.getUri().normalize())) {
			return path;
		}
		
		Path tmpDest = new Path( path.getName() + "." + System.currentTimeMillis());
		if (!FileUtil.copy(pathFs, path, defaultFs, tmpDest, false, conf)) {
			throw new IOException( "can't copy from " + path + " to " + tmpDest);
		}
		return tmpDest;
	}
	
	/** Create a local tmp file with the file name as contents
	 * 
	 * 
	 * @param conf The Configuration object to use to work out how to access the file system of file
	 * @param deleteOnExit Mark the file to be deleted on exit
	 * @return The path created
	 * 
	 * @throws IOException
	 */
	public static Path createSimpleTmpFile( final Configuration conf, boolean deleteOnExit ) throws IOException
	{
		/** Make a temporary File, delete it, mark the File Object as delete on exit, and create the file with data using hadoop utils. */
		File localTempFile = File.createTempFile("tmp-file", "tmp");
		localTempFile.delete();
		Path localTmpPath = new Path( localTempFile.getName());
		
		createSimpleFile(conf, localTmpPath, localTmpPath.toString());
		if (deleteOnExit) {
			localTmpPath.getFileSystem(conf).deleteOnExit(localTmpPath);
		}
		return localTmpPath;
	}


	/** Given a set of files, create a zip file from them.
	 * 
	 * @param conf The Configuration object ot use for file system access
	 * @param zipFile The path to create as a zip file
	 * @param files The set of files to archive
	 * @param pathTrimCount The number of leading components to trim from the file path name for the zip file entry
	 * @param isJar If true, create a jar, not a zip
	 * @param jarManifest if isJar is true this may be non null and is used as a manifest for the jar
	 * @throws IOException
	 */
	public static void createArchiveFile( final Configuration conf, final Path zipFile, final List<Path> files, int pathTrimCount, boolean isJar, Manifest jarManifest) throws IOException
	{
		
		FSDataOutputStream out = null;
		ZipOutputStream zipOut = null;
		/** Did we actually write an entry to the zip file */
		boolean wroteOne = false;
		
		try {
			out = zipFile.getFileSystem(conf).create(zipFile);
			if (files.isEmpty()) {
				return;	/** Don't try to create a zip file with no entries it will throw. just return empty file. */
			}
			if (isJar) {
				if (jarManifest!=null) {
					zipOut = new JarOutputStream(out,jarManifest);
				} else {
					zipOut = new JarOutputStream(out);
				}
			} else {
				zipOut = new ZipOutputStream(out);
			}
			for ( Path file : files) {
				FSDataInputStream in = null;
				/** Try to complete the file even if a file fails. */
				try {
					FileSystem fileSystem = file.getFileSystem(conf);
					in = fileSystem.open(file);
					ZipEntry zipEntry = new ZipEntry(pathTrim(file, pathTrimCount));
					zipOut.putNextEntry(zipEntry);
					IOUtils.copyBytes(in, zipOut, (int) Math.min(32768, fileSystem.getFileStatus(file).getLen()), false);
					wroteOne = true;
				} catch( IOException e) {
					LOG.error( "Unable to store " + file + " in zip file " + zipFile + ", skipping", e);
					continue;
				} finally {
					if (in!=null) {
						try { in.close(); } catch( IOException ignore ) {}
					}
				}
			}
		} finally { /** Ensure everything is always closed before the function exits */
			
			if (zipOut!=null&&wroteOne) {
				zipOut.closeEntry();
				zipOut.flush();
				zipOut.close();
				out = null;
			}
			if (out!=null) {
				out.close();
			}
		}
	}
	/** Return the trailing path component of a file with a number of leading elements removed.
	 * 
	 * If the number of elements to remove is greater than or equal to the number 
	 * of path components, only the last component of the file will be returned.
	 * 
	 * <em>Note:</em> this relies on {@link Path#SEPARATOR} to determine path components.
	 * 
	 * @param file The path to work on
	 * @param pathTrimCount The number of elements to remove
	 * @return The trailing component
	 */
	public static String pathTrim(Path file, int pathTrimCount) {
		if (file==null) {
			LOG.error("Null path in pathTrim");
			return "";
		}
		if (pathTrimCount==0) {
			return file.toString();
		}
		String[] parts = StringUtils.split(file.toString(), Path.SEPARATOR);
		
		if (parts.length<=pathTrimCount) {/** Only return the filename portion. */
			return parts[parts.length-1];
		}
		return StringUtils.join(parts, Path.SEPARATOR, pathTrimCount, parts.length);
	}

	/** If the job is configured for text output, and the output directory is not configured, set it, and optionally delete it if it exists.
	 * 
	 * @param job The JobConf object to operate on
	 * @param outputDirectory The name of the output directory to use
	 * @param deleteIf if true, delete the output directory if it exists.
	 * @throws IOException If there is a failure manipulating the output directory
	 */
	public static void setupAndRemoveOutputIf(final JobConf job, String outputDirectory, boolean deleteIf) throws IOException {
		
		if (job.getOutputFormat() instanceof FileOutputFormat) {
			Path outputPath = new Path(outputDirectory);
			if (FileOutputFormat.getOutputPath(job)==null) {
				FileOutputFormat.setOutputPath(job, outputPath);
			}
			if (deleteIf) {
				FileSystem fs = outputPath.getFileSystem(job);
				if (fs.exists(outputPath)) {
					fs.delete(outputPath, true);
				}
			}
		}
	}

	/** Create a zipFile in the system temporary directory, with temporary files as data.
	 * 
	 * @param conf The Configuration object to use for file access
	 * @param count The number of files to create in the zip
	 * @param isJar Create a jar file if this is true
	 * @return The zipFile path
	 * @throws IOException
	 */
	public static Path setupArchiveFile( final Configuration conf, final int count, boolean isJar ) throws IOException {
		File tmpDir = File.createTempFile("tmpDir", "");
		if (!tmpDir.delete()) {
			throw new IOException("Can not delete recently created tmpFile");
		}
		if (!tmpDir.mkdirs()) {
			throw new IOException("Can not create tmp directory " + tmpDir);
		}
		
		final Path tmpDirPath = new Path(tmpDir.getName());
		List<Path> files = createSimpleDirectory(conf, tmpDirPath, count, null);
		final Path zipFile = tmpDirPath.suffix(isJar?".jar":".zip");
		createArchiveFile(conf, zipFile, files, 1, isJar, null);
		/** Now clean up the tmpDir. */
		FileSystem fs = tmpDirPath.getFileSystem(conf);
		fs.delete( tmpDirPath, true);
		return zipFile;
	}

	/** Return the set of names in a zip file.
	 * 
	 * @param zipFile The zip file to examine
	 * @param conf The conf object to use
	 * @return the list of names, if zipFile is null the empty list is returned
	 * @throws IOException
	 */
	public static List<String> getZipEntries( final Path zipFile, final Configuration conf) throws IOException
	{
		if (zipFile==null) {
			return Collections.<String>emptyList();
		}
		FSDataInputStream in = null;
		ArrayList<String> entries = new ArrayList<String>();
		try {
			in = zipFile.getFileSystem(conf).open(zipFile);
			ZipInputStream zin = new ZipInputStream(in);
			ZipEntry nextEntry;
			while ((nextEntry = zin.getNextEntry())!=null) {
				entries.add(nextEntry.getName());
			}
			return entries;	
		} finally {
			if (in!=null) {
				in.close();
			}
		}
	}
	
	/** Given a path, add the trailing #name to tell the {@link DistributedCache} the symlink name.
	 * 
	 * @param path The path component to convert to a URI with a fragment for symlinking.
	 * @return
	 * @throws URISyntaxException
	 */
	public static URI addSymLinkFragment(Path path) throws URISyntaxException {
		URI pathURI = path.toUri();
		return
			new URI( pathURI.getScheme(),
					pathURI.getAuthority(),
					pathURI.getHost(),
					pathURI.getPort(),
					pathURI.getPath(),
					pathURI.getQuery(),
					path.getName()	/* This is the magic bit in all this boilerplate. */
					);
	}


	/** This method provides an alternate filename to look for for items passed via the {@link DistributedCache}.
	 * 
	 * At least through Hadoop 0.19.0, the DistributedCache may change the file name portion of paths that are being passed
	 * via the distributed cache.
	 * This method provides the alternate name for a given item.
	 * 
	 * @param name The file name portion of a path to a file passed via the distributed cache
	 * @param conf The JobConfiguration object
	 * @return A candidate file name for <code>name</code> in the distributed cache.
	 */
	public static String makeRelativeName( final String name, final Configuration conf) {
		try {
			URI nameUri = new URI(name);
			String nameRelative = DistributedCache.makeRelative(nameUri,conf);
			return nameRelative;
		} catch( URISyntaxException ignore) {
			LOG.debug("Unable to convert cache file name " + name + " to URI for DistributedCache relative check", ignore);
		} catch (IOException ignore) {
			LOG.debug("Unable to convert cache file name " + name + "  DistributedCache relative name", ignore);
		}
		return null;
	}


	/** Look for the distributed cache archive that is not in the classpath, that has the file name <code>name</code>
	 * 
	 * This Method does a simple linear search and will be slow if run repeatedly or on large caches.
	 * @param name The file name to lookup, this must only be the file name, no directory components.
	 * @param conf The JobConf to use
	 * @return the local path, or null if the item was not found
	 * @exception If there is an issue with the lookup.
	 */
	public static Path findNonClassPathArchive( final String name, final Configuration conf) throws IOException {
		if (name==null) {
			return null;
		}
		Path []archives = DistributedCache.getLocalCacheArchives(conf);
		if (archives==null) {
			return null;
		}
		String altName = makeRelativeName(name, conf);
		for( Path archive : archives) {
			if (name.equals(archive.getName())) {
				return archive;
			}
			if (altName!=null&&altName.equals(archive.getName())) {
				return archive;
			}
		}
		return null;
	}

	/** Look for the distributed cache archive that is in the classpath, that has the file name <code>name</code>
	 * 
	 * This Method does a simple linear search and will be slow if run repeatedly or on large caches.
	 * @param name The file name to lookup, this must only be the file name, no directory components.
	 * @param conf The JobConf to use
	 * @return the local path, or null if the item was not found
	 */

	public static Path findClassPathArchive( final String name, final Configuration conf) {
		if (name==null) {
			return null;
		}
		Path []archives = DistributedCache.getArchiveClassPaths(conf);
		if (archives==null) {
			return null;
		}
		for( Path archive : archives) {
			if (name.equals(archive.getName())) {
				return archive;
			}
		}
		return null;
	}

	/** Look for the distributed cache archive that is not in the classpath, that has the file name <code>name</code>
	 * 
	 * This Method does a simple linear search and will be slow if run repeatedly or on large caches.
	 * @param name The file name to lookup, this must only be the file name, no directory components.
	 * @param conf The JobConf to use
	 * @return the local path, or null if the item was not found
	 * @exception If there is an issue with the lookup.
	 */

	public static Path findNonClassPathFile( final String name, final Configuration conf) throws IOException {
		if (name==null) {
			return null;
		}
		Path []files = DistributedCache.getLocalCacheFiles(conf);
		if (files==null) {
			return null;
		}
		String altName = makeRelativeName(name, conf);
		for( Path file : files) {
			if (name.equals(file.getName())) {
				return file;
			}
			if (altName!=null&&altName.equals(file.getName())) {
				return file;
			}
		}
		return null;
	}

	/** Look for the distributed cache file that is in the classpath, that has the file name <code>name</code>
	 * 
	 * This Method does a simple linear search and will be slow if run repeatedly or on large caches.
	 * @param name The file name to lookup, this must only be the file name, no directory components.
	 * @param conf The JobConf to use
	 * @return the local path, or null if the item was not found
	 */
	public static Path findClassPathFile( final String name, final Configuration conf) {
		if (name==null) {
			return null;
		}
		Path []files = DistributedCache.getFileClassPaths(conf);
		if (files==null) {
			return null;
		}
		for( Path file : files) {
			if (name.equals(file.getName())) {
				return file;
			}
		}
		return null;
	}


	/** Given the filename portion of a path that was distributed via the DistributedCache, return the current local path.
	 * 
	 * This method looks in the classpath file list, the non classpath file list, the classpath archive list and then the non classpath archive list.
	 * 
	 * This Method does a simple linear search and will be slow if run repeatedly or on large caches.
	 * @param name The file name to lookup, this must only be the file name, no directory components.
	 * @param conf The JobConf to use
	 * @return the local path, or null if the item was not found
	 * @exception If there is an issue with the lookup.
	 */
	public static Path findItemInCache( final String name, final Configuration conf) throws IOException {
		if (name==null) {
			return null;
		}
		Path result = null;
		result = findClassPathFile(name,conf);
		if (result!=null) {
			return result;
		}
		result = findNonClassPathFile(name,conf);
		if (result!=null) {
			return result;
		}
		result = findClassPathArchive(name,conf);
		if (result!=null) {
			return result;
		}
		result = findNonClassPathArchive(name,conf);
		if (result!=null) {
			return result;
		}
		return null;
	}

	/** Helper method to close a {@link Closeable} object if it is not null
	 * 
	 * Exceptions are caught and ignored. If the log level is debug, exceptions will be logged.
	 * @param out The {@link Closeable} to close if it is not null.
	 */
	public static void closeIf( Closeable out ) {
		if (out!=null) {
			try {
				out.close();
			} catch (IOException ignore) {
				if (LOG.isDebugEnabled()) {
					String outName = "unknown";
					try {
						outName = out.toString();
					} catch( Throwable ignore1 ) {
						try {
							outName = "" + out.hashCode();
						} catch( Throwable ignore2 ) {}
					}
					LOG.debug( "closeIf of closeable [" + outName + "] failed, ignored", ignore);
				}
			}
		}
	}



}
