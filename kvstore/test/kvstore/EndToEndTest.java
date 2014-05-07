package kvstore;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class EndToEndTest extends EndToEndTemplate {

    @Test
    public void testPutGet() throws KVException {
        client.put("foo", "bar");
        assertEquals(client.get("foo"), "bar");
    }
    
    @Test
    public void testPutGet1() throws KVException {
        client.put("foo", "bar");
        client.put("foo", "blah");
        assertEquals(client.get("foo"), "blah");
    }
    
    @Test
    public void testPutGet2() throws KVException {
        client.put("foo", "bar");
        assertEquals(client.get("foo"), "bar");
        client.put("foo", "blah");
        assertEquals(client.get("foo"), "blah");
    }

}
