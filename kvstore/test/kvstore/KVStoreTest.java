package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;

import org.junit.*;

public class KVStoreTest {

    KVStore store;

    @Before
    public void setupStore() {
        store = new KVStore();
    }

    @Test
    public void putAndGetOneKey() throws KVException {
        String key = "this is the key.";
        String val = "this is the value.";
        store.put(key, val);
        assertEquals(val, store.get(key));
    }
    
    @Test
    public void dumpAndRestore0() throws KVException {
        String key = "this is the key.";
        String val = "this is the value.";
        store.put(key, val);
        
        store.dumpToFile("test0");
        store.restoreFromFile("test0");
        //System.out.println(store.toString());
        
        //System.out.println(store.toString());
        
        assertEquals(val, store.get(key));
    }
    
    @Test
    public void dumpAndRestore1() throws KVException {

        store.put("1", "one");
        store.put("2", "two");
        store.put("3", "three");
        store.put("4", "four");
        
        store.dumpToFile("test1");
        store.restoreFromFile("test1");
        //System.out.println(store.toString());
        
        assertEquals("one", store.get("1"));
        assertEquals("two", store.get("2"));
        assertEquals("three", store.get("3"));
        assertEquals("four", store.get("4"));
    }
    
    @Test
    public void dumpAndRestore2() throws KVException {

        store.restoreFromFile("test1");
        //System.out.println(store.toString());
        
        assertEquals("one", store.get("1"));
        assertEquals("two", store.get("2"));
        assertEquals("three", store.get("3"));
        assertEquals("four", store.get("4"));
    }

}
