package kvstore;

import static org.junit.Assert.*;

import org.junit.*;

public class KVCacheTest {

    /**
     * Verify the cache can put and get a KV pair successfully.
     */
    @Test
    public void singlePutAndGet() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
    }

    /**
     * Verify the cache can put and replace a KV pair successfully.
     */
    @Test
    public void singlePutAndReplace() {
    	KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
        cache.put("hello", "friend");
        assertEquals("friend", cache.get("hello"));
    }
    
    /**
     * Verify the cache can put and delete a KV pair successfully.
     */
    @Test
    public void singlePutAndDel() {
    	KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
        cache.del("hello");
        assertEquals(null, cache.get("hello"));
    }
    
    /**
     * Verify the cache can change reference bit correctly.
     */
    @Test
    public void referenceTest() {
    	KVCache cache = new KVCache(1, 4);
    	
    	//Get
        cache.put("hello", "world");
        assertEquals(0, cache.getReference("hello"));
        assertEquals("world", cache.get("hello"));
        assertEquals(1, cache.getReference("hello"));
        
        //Put and replace
        assertEquals(-1, cache.getReference("hi"));
        cache.put("hi", "friend");
        assertEquals(0, cache.getReference("hi"));
        cache.put("hi", "buddy");
        assertEquals("buddy", cache.get("hi"));
        assertEquals(1, cache.getReference("hi"));
    }
    
    /**
     * Verify the cache implements second chance correctly.
     */
    @Test
    public void secondChance() {
    	KVCache cache = new KVCache(1, 4);
    	cache.put("1", "one");
    	cache.put("2", "two");
    	cache.put("3", "three");
    	cache.put("4", "four");
    	cache.get("1");
    	cache.get("2");
    	cache.get("4");
    	cache.put("5", "five");
    	
    	assertEquals(null, cache.get("3"));
    	assertEquals("five", cache.get("5"));
    	assertEquals("one", cache.get("1"));
    	assertEquals("two", cache.get("2"));
    	assertEquals("four", cache.get("4"));
    	
    	assertEquals(0, cache.getReference("1"));
    	assertEquals(0, cache.getReference("2"));
    	assertEquals(0, cache.getReference("5"));
    	assertEquals(1, cache.getReference("4"));
    }
    
    /**
     * Verify the xml creation works.
     */
    @Test
    public void xmlTest1() {
    	KVCache cache = new KVCache(1, 4);
    	cache.put("1", "one");
    	cache.put("2", "two");
    	cache.put("3", "three");
    	cache.put("4", "four");
    	cache.get("1");
    	cache.get("2");
    	cache.get("4");
    	cache.put("5", "five");
    	
    	System.out.println(cache.toString());
    }
    
    /**
     * Verify the xml creation works.
     */
    @Test
    public void xmlTest2() {
    	KVCache cache = new KVCache(2, 4);
    	cache.put("1", "one");
    	cache.put("2", "two");
    	cache.put("3", "three");
    	cache.put("4", "four");
    	cache.put("1", "ONE");
    	cache.get("2");
    	cache.get("4");
    	
    	System.out.println(cache.toString());
    }
    
}
