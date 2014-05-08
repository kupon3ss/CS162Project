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
    		KVServer server = new KVServer(1, 4);
    		TPCLog log = new TPCLog("test", server);
    		KVMessage request = new KVMessage(KVConstants.PUT_REQ);
    		request.setKey("foo");
    		request.setValue("bar");
    		log.appendAndFlush(request);
    		request = new KVMessage(KVConstants.PUT_REQ);
    		request.setKey("hello");
    		request.setValue("world");
    		log.appendAndFlush(request);
    		
    		log = new TPCLog("test", new KVServer(1,4));
    		log.rebuildServer();
    		
    	} catch (KVException kve) {
    		fail("Test should not fail");
    	}
    }
}
