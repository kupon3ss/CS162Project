package kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

public class TPCEndToEndTest extends TPCEndToEndTemplate {

    @Test(timeout = 15000)
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals("get failed", client.get("foo"), "bar");
    }
    
    @Test(timeout = 15000)
    public void testPutDel() throws KVException {
    	try {
	        client.put("foo", "bar");
	        client.del("foo");
	        client.get("foo");
    	} catch (KVException kve) {
    		assertEquals("del failed",  KVConstants.ERROR_NO_SUCH_KEY, kve.getKVMessage().getMessage());
    	}
    }
    
    @Test(timeout = 15000)
    public void test0() throws KVException {
    	try {
	        client.put("foo", "bar");
	        client.get(null);
    	} catch (KVException kve) {
    		assertEquals("del failed",  KVConstants.ERROR_INVALID_KEY, kve.getKVMessage().getMessage());
    	}
    }
    
    @Test(timeout = 15000)
    public void test1() throws KVException {
    	try {
	        client.put("foo", "bar");
	        assertEquals(client.get("foo"), "bar");
	        client.put("hello", "world");
	        assertEquals(client.get("hello"), "world");
    	} catch (KVException kve) {
    		
    	}
    }
    
    @Test
    public void fuzzTest() throws KVException {
        Random rand = new Random(8); // no reason for 8
        Map<String, String> map = new HashMap<String, String>(4);
        String key, val;
        for (int i = 0; i < 4; i++) {
            key = Integer.toString(rand.nextInt());
            val = Integer.toString(rand.nextInt());
            client.put(key, val);
            map.put(key, val);
        }
        Iterator<Map.Entry<String, String>> mapIter = map.entrySet().iterator();
        Map.Entry<String, String> pair;
        while(mapIter.hasNext()) {
            pair = mapIter.next();
            if (!client.get(pair.getKey()).equals(pair.getValue())) {System.out.println("failed fuzz Test"); return;}
            mapIter.remove();
        }
        assertTrue(map.size() == 0);
        System.out.println("YAYPASSFUZZTEST");
    }
}
