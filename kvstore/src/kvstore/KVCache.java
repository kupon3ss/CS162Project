package kvstore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

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
	int maxElemsPerSet;

    /**
     * Constructs a second-chance-replacement cache.
     *
     * @param numSets the number of sets this cache will have
     * @param maxElemsPerSet the size of each set
     */
    public KVCache(int numSets, int maxElemsPerSet) {
    	cache = new ArrayList<LinkedList<CacheEntry>>(numSets);
    	locks = new ReentrantLock[numSets];
    	for (int i = 0; i < numSets; i++) {
    		cache.add(new LinkedList<CacheEntry>());
    		locks[i] = new ReentrantLock();
    	}
    	this.maxElemsPerSet = maxElemsPerSet;
    }

    public void freshCache() {
    	int numSets = cache.size();
        cache = new ArrayList<LinkedList<CacheEntry>>(numSets);
        for (int i = 0; i < numSets; i++) {
    		cache.add(new LinkedList<CacheEntry>());
    		locks[i] = new ReentrantLock();
    	}
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
        for (CacheEntry entry : set) {
            if (entry.getKey().equals(key)) {
                entry.setRefTrue();
                return entry.getValue();
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
    	boolean replaced = false;
    	
    	//Checks if key already exists in set
        for (CacheEntry entry : set) {
            if (entry.getKey().equals(key)) {
                entry.setValue(value);
                entry.setRefTrue();
                replaced = true;
            }
        }
    	
    	//Should only run if key did not exist and key, value not replaced
    	//Either use second chance algorithm or append onto end of list
    	if (!replaced) {
            if (set.size() != maxElemsPerSet) {
                set.add(new CacheEntry(key, value));
                return;
            }
            CacheEntry secondChance;
    		while (true) {
        		if (set.getFirst().hasRef()) {
       				//Gives second chance. Removes head and adds onto tail with ref bit false
       				secondChance = set.remove();
        			secondChance.setRefFalse();
        			set.addLast(secondChance);
       			} else {
        			//No second chance so remove head and adds new entry onto tail
        			set.remove();
        			set.add(new CacheEntry(key, value));
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
        Iterator<CacheEntry> set = cache.get(getSetId(key)).iterator();
        while (set.hasNext()) {
            if (set.next().getKey().equals(key)) set.remove();
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
    		
    		
    		Element root = xmlDoc.createElement("KVCache");
    		for (int i = 0; i < cache.size(); i++) {
    			Element setXML = xmlDoc.createElement("Set");
    			setXML.setAttribute("Id", Integer.toString(i));
    			
    			LinkedList<CacheEntry> set = cache.get(i);
                for (CacheEntry e : set) {
                    Element entry = xmlDoc.createElement("CacheEntry");
                    entry.setAttribute("isReferenced", String.valueOf(e.hasRef()));

                    Element key = xmlDoc.createElement("Key");
                    key.appendChild(xmlDoc.createTextNode(e.getKey()));
                    Element value = xmlDoc.createElement("Value");
                    value.appendChild(xmlDoc.createTextNode(e.getValue()));

                    entry.appendChild(key);
                    entry.appendChild(value);

                    setXML.appendChild(entry);
                }
    			root.appendChild(setXML);
    		}
    		xmlDoc.appendChild(root);
    		
    		Transformer transformer = null;
    	    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    		try {
    			transformer = transformerFactory.newTransformer();
    		} catch (TransformerConfigurationException e) {
    		}
    		
    		StringWriter xmlwriter = new StringWriter();
    		DOMSource source = new DOMSource(xmlDoc);
    		StreamResult xmlout = new StreamResult(xmlwriter);
    		
    		try {
    			transformer.transform(source, xmlout);
    		} catch (TransformerException|NullPointerException e) {
    		}
    		
    		xmlDoc.setXmlStandalone(true);
    		return xmlwriter.toString();
    	} catch (ParserConfigurationException e) {
    		return null;
    	}
    }

    @Override
    public String toString() {
        return this.toXML();
    }
    
    /**
     * Testing reference purposes. Get the reference bit of given key.
     * Returns -1 if doesn't exist in set. Otherwise, returns 0 for false or 1 for true. 
     */
    public int getReference(String key) {
    	LinkedList<CacheEntry> set = cache.get(getSetId(key));
        for (CacheEntry entry : set) {
            if (entry.getKey().equals(key)) {
                return entry.hasRef() ? 1 : 0;
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
    	
    	public boolean hasRef() {
    		return this.reference;
    	}
    }
    
}
