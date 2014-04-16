package kvstore;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.*;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is removed based on
 * the eviction policy.
 */
public class KVCache implements KeyValueInterface {
	
	ArrayList<LinkedList<CacheEntry>> cache;
	ReentrantLock[] locks;

    /**
     * Constructs a second-chance-replacement cache.
     *
     * @param numSets the number of sets this cache will have
     * @param maxElemsPerSet the size of each set
     */
    public KVCache(int numSets, int maxElemsPerSet) {
    	for (int i = 0; i < numSets; i++) {
    		cache.add(createSet(maxElemsPerSet));
    	}
    }
    
    //Creates a set of null CacheEntries.
    public LinkedList<CacheEntry> createSet(int numEntries) {
    	LinkedList<CacheEntry> set = new LinkedList<CacheEntry>();
    	for (int i = 0; i < numEntries; i++) {
    		set.add(null);
    	}
    	return set;
    }

    /**
     * Retrieves an entry from the cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method.
     *
     * @param  key the key whose associated value is to be returned.
     * @return the value associated to this key or null if no value is
     *         associated with this key in the cache
     */
    @Override
    public String get(String key) {
    	LinkedList<CacheEntry> set = cache.get(getSetId(key));
    	for (int i = 0; i < set.size(); i++) {
    		if (set.get(i) != null) {
    			if (set.get(i).getKey() == key) {
    				set.get(i).setRefTrue();
    				return set.get(i).getValue();
    			}
    		}
    	}
    	return null;
    }

    /**
     * Adds an entry to this cache.
     * If an entry with the specified key already lives in the cache, it is
     * replaced by the new entry. When an entry is replaced, its reference bit
     * will be set to True. If the set is full, an entry is removed from
     * the cache based on the eviction policy. If the set isn't full, the entry
     * will be inserted behind all existing entries. For this policy, we suggest
     * using a LinkedList over an array to keep track of entries in a set since
     * deleting an entry in an array will leave a hole in the array, likely not
     * at the end. More details and explanations in the spec. Assumes access to
     * the corresponding set has already been locked by the caller of this
     * method.
     *
     * @param key the key with which the specified value is to be associated
     * @param value a value to be associated with the specified key
     */
    @Override
    public void put(String key, String value) {
    	LinkedList<CacheEntry> set = cache.get(getSetId(key));
    	int empty = -1;
    	
    	//Either retrieves the first null spot or replaces key, value pair
    	for (int i = 0; i < set.size(); i++) {
    		if (set.get(i) != null) {
    			if (set.get(i).getKey() == key) {
    				set.get(i).setValue(value);
    				set.get(i).setRefTrue();
    			}
    		} else if(empty == -1) {
    			empty = i;
    		}
    	}
    	
    	//Either puts key, value pair at null spot or uses second chance algorithm to replace element
    	if (empty != -1) {
    		set.remove(empty);
    		set.add(empty, new CacheEntry(key, value));
    	} else {
    		//Set must be full here
    		while (true) {
    			CacheEntry secondChance; 
    			if (set.getFirst().getRef()) {
    				//Gives second chance. Removes head and adds onto tail with ref bit false
    				secondChance = set.remove();
    				secondChance.setRefFalse();
    				set.addLast(secondChance);
    			} else {
    				//No second chance so remove head and adds new entry onto tail
    				set.remove();
    				set.addLast(new CacheEntry(key, value));
    				break;
    			}
    		}
    	}
    	
    }

    /**
     * Removes an entry from this cache.
     * Assumes usage of the corresponding set has already been locked by the
     * caller of this method. Does nothing if called on a key not in the cache.
     *
     * @param key key with which the specified value is to be associated
     */
    @Override
    public void del(String key) {
    	LinkedList<CacheEntry> set = cache.get(getSetId(key));
    	for (int i = 0; i < set.size(); i++) {
    		if (set.get(i) != null) {
    			if (set.get(i).getKey() == key) {
    				set.remove(i);
    				set.add(null);
    			}
    		}
    	}
    }

    /**
     * Get a lock for the set corresponding to a given key.
     * The lock should be used by the caller of the get/put/del methods 
     * so that different sets can be changed in parallel.
     *
     * @param  key key to determine the lock to return
     * @return lock for the set that contains the key
     */
    public Lock getLock(String key) {
    	return locks[getSetId(key)];
    }

    /**
     * Get the id of the set for a specific key.
     *
     * @param  key key of interest
     * @return set of the key
     */
    private int getSetId(String key) {
        return Math.abs(key.hashCode() % cache.size());
    }

    /**
     * Serialize this store to XML. See spec for details on output format.
     */
    public String toXML() {
    	try {
    		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    		DocumentBuilder db = dbf.newDocumentBuilder();
    		Document xmlDoc = db.newDocument();
    		xmlDoc.setXmlStandalone(true);
    		
    		Element root = xmlDoc.createElement("KVCache");
    		for (int i = 0; i < cache.size(); i++) {
    			Element setXML = xmlDoc.createElement("Set");
    			setXML.setAttribute("Id", Integer.toString(i));
    			
    			LinkedList<CacheEntry> set = cache.get(i);
    			for (int j = 0; j < set.size(); j++) {
    				Element entry = xmlDoc.createElement("CacheEntry");
    				entry.setAttribute("isReferenced", String.valueOf(set.get(j).getRef()));
    				
    				Element key = xmlDoc.createElement("Key");
    				key.setNodeValue(set.get(j).getKey());
    				Element value = xmlDoc.createElement("Value");
    				value.setNodeValue(set.get(j).getValue());
    				
    				entry.appendChild(key);
    				entry.appendChild(value);
    				
    				setXML.appendChild(entry);
    			}
    			root.appendChild(setXML);
    		}
    		xmlDoc.appendChild(root);
    		return xmlDoc.getXmlEncoding();
    	} catch (ParserConfigurationException e) {
    		return null;
    	}
    }

    @Override
    public String toString() {
        return this.toXML();
    }
    
    /**
     * 
     * @author MichaelChuang
     * Testing reference purposes. Get the reference bit of given key.
     * Returns -1 if doesn't exist in set. Otherwise, returns 0 for false or 1 for true. 
     */
    public int getReference(String key) {
    	LinkedList<CacheEntry> set = cache.get(getSetId(key));
    	for (int i = 0; i < set.size(); i++) {
    		if (set.get(i) != null) {
    			if (set.get(i).getKey() == key) {
    				set.get(i).setRefTrue();
    				if (set.get(i).getRef()) {
    					return 1;
    				} else {
    					return 0;
    				}
    			}
    		}
    	}
    	return -1;
    }

    private class CacheEntry {
    	
    	private String key, value;
    	private boolean reference;
    	
    	public CacheEntry(String k, String v) {
    		this.key = k;
    		this.value = v;
    		this.reference = false;
    	}
    	
    	public void setRefTrue() {
    		this.reference = true;
    	}
    	
    	public void setRefFalse() {
    		this.reference = false;
    	}
    	
    	public String getValue() {
    		return this.value;
    	}
    	
    	public void setValue(String v) {
    		this.value = v;
    	}
    	
    	public String getKey() {
    		return this.key;
    	}
    	
    	public boolean getRef() {
    		return this.reference;
    	}
    }
    
}
