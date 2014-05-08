package kvstore;

import static org.junit.Assert.*;

import org.junit.*;

public class TPCLogTest {
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
    		System.out.println(server.toString());
    		assertEquals("world", server.get("hello"));
    		assertEquals("bar", server.get("foo"));
    		
    	} catch (KVException kve) {
    		fail("Test should not fail");
    	}
    }
}
