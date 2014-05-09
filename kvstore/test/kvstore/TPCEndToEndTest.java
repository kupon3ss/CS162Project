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
    
}
