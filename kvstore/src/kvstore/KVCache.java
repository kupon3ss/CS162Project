package kvstore;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is removed based on
 * the eviction policy.
 */
public class KVCache implements KeyValueInterface {

    /**
     * Constructs a second-chance-replacement cache.
     *
     * @param numSets the number of sets this cache will have
     * @param maxElemsPerSet the size of each set
     */
    public KVCache(int numSets, int maxElemsPerSet) {
        // implement me
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
        // implement me
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
        // implement me
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
        // implement me
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
        // implement me
        return null;
    }

    /**
     * Get the id of the set for a specific key.
     *
     * @param  key key of interest
     * @return set of the key
     */
    private int getSetId(String key) {
        // implement me
        return 0;
    }

    /**
     * Serialize this store to XML. See spec for details on output format.
     */
    public String toXML() {
        // implement me
        return null;
    }

    @Override
    public String toString() {
        return this.toXML();
    }

}
