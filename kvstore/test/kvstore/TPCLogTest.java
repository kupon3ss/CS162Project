package kvstore;

import static org.junit.Assert.*;

import org.junit.*;

public class TPCLogTest extends TPCEndToEndTemplate{
	/**
     * Sanity test that TPCLog can be rebuilt correctly.
     */
    @Test
    public void rebuildTest() {
    	try {
    		KVServer server;
    		TPCLog log = new TPCLog("rebuildTest", new KVServer(1, 4));
    		KVMessage request = new KVMessage(KVConstants.PUT_REQ);
    		KVMessage decision = new KVMessage(KVConstants.COMMIT);
    		request.setKey("foo");
    		request.setValue("bar");
    		log.appendAndFlush(request);
    		log.appendAndFlush(decision);
    		request = new KVMessage(KVConstants.PUT_REQ);
    		decision = new KVMessage(KVConstants.COMMIT);
    		request.setKey("hello");
    		request.setValue("world");
    		log.appendAndFlush(request);
    		log.appendAndFlush(decision);

    		
    		log.rebuildServer();
    		server = log.getServer();
    		//System.out.println(server.toString());
    		assertEquals("world", server.get("hello"));
    		assertEquals("bar", server.get("foo"));
    		
    	} catch (KVException kve) {
    		fail("Test should not fail");
    	}
    }
    
    @Test
    public void setup() {
    	try {
	    	client.put("one", "1");
	    	client.put("two", "2");
	    	client.put("three", "3");
	    	client.put("four", "4");
	    	client.put("five", "5");
	    	client.put("six", "6");
	    	//client.put("seven", "7");
	    	client.del("six");
	    	client.put("eight", "8");
    	} catch (KVException kve) {
    		
    	}
    }
    
    //Don't forget to delete the log files before running this test in bin folder
    @Test
    public void check() {
    	try {
	    	assertEquals(client.get("one"), "1");
	    	assertEquals(client.get("two"), "2");
	    	assertEquals(client.get("three"), "3");
	    	assertEquals(client.get("four"), "4");
	    	assertEquals(client.get("five"), "5");
	    	//assertEquals(client.get("six"), "6");
	    	//assertEquals(client.get("seven"), "7");
	    	assertEquals(client.get("eight"), "8");
	    	client.get("six");
    	} catch (KVException kve) {
    		assertEquals(kve.getKVMessage().getMessage(), KVConstants.ERROR_NO_SUCH_KEY);
    	}
    }
}
