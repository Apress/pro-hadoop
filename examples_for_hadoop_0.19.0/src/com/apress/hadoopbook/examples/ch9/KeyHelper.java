package com.apress.hadoopbook.examples.ch9;

/** A interface to provide services for key objects.
 * It is expected that different actual objects will provide raw
 * key data, this helper class provides a stylized way of
 * packing and unpacking the relevant key data from the underlying
 * transport form.
 * 
 * It is also expected that the application will evolve and different
 * types will be used for the transport.
 * 
 * The instances must be designed to allow object reuse, to avoid
 * churning huge numbers of these during a map or reduce.
 * 
 * @author Jason
 *
 * @param <K>
 */
public interface KeyHelper<K> {
	/** Initialize the helper with a new key from <code>raw</code>.
	 * 
	 * @param raw The object to initialize the key helper
	 * @return true if raw contained a valid key.
	 */
	public boolean getFromRaw(K raw);
	/** Pack the key data from this object into <code>raw</code>
	 * @param raw The key object to set to the value of the helper 
	 */
	public void setToRaw(K raw);
	
	/** Is the key a search request key
	 * @return true if the key is a key to search for.
	 */
	public boolean isSearchRequest();
	
	/** Is the key that this has been set to, a Search Space key.
	 * @return true if the key is a search space key i.e. has a begin and end range.
	 */
	public boolean isSearchSpace();
	
	/** If this helper contains a valid key.
	 * 
	 * @return true if this is a valid key.
	 */
	public boolean isValid();
	
	/** If the underlying key is a search request key, return the
	 * actual request data. If the key is a search space key, the behavior is undefined.
	 * 
	 * @return The search request data.
	 */
	public long getSearchRequest();
	
	
	/** Return the beginning range of the search space key.
	 * If the key is a search request key, the behavior is undefined,
	 * it is suggested that the request key be returned.
	 * @return The begin range key of a search space key.
	 */
	public long getBeginRange();
	
	/** Return the ending range of the search space key.
	 * If the key is a search request key, the behavior is undefined,
	 * it is suggested that the request key be return
	 * @return
	 */
	public long getEndRange();
	
	/** Loose any state information.
	 * 
	 */
	public void reset();
	
	
	/** Set the beginning address of this search space record, mark this as a SearchSpace and valid item.
	 * 
	 * @param beginRange
	 */
	public void setBeginRange(final long beginRange);
	
	/** Set the end address of this search space record,  mark this as a SearchSpace and valid item.
	 * @param endRange
	 */
	public void setEndRange(final long endRange);	

	
	/** Set the address for a search request,  mark this as a Search Request and valid item.
	 * @param searchRequest
	 */
	public void setSearchRequest(final long searchRequest);
}
