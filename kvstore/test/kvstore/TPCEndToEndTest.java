package kvstore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TPCEndToEndTest extends TPCEndToEndTemplate {

    @Test(timeout = 15000)
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals("get failed", client.get("foo"), "bar");
    }
    
    @Test(timeout = 15000)
    public void testPutDel() throws KVException {
        client.put("foo", "bar");
        client.del("foo");
        assertEquals("del failed", client.get("foo"), KVConstants.ERROR_NO_SUCH_KEY);
    }
}
