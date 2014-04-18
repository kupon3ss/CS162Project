package kvstore;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.*;

public class KVServerTest {

    KVServer server;

    @Before
    public void setupServer() {
        server = new KVServer(10, 10);
    }

    @Test
    public void fuzzTest() throws KVException {
        Random rand = new Random(8); // no reason for 8
        Map<String, String> map = new HashMap<String, String>(10000);
        String key, val;
        for (int i = 0; i < 10000; i++) {
            key = Integer.toString(rand.nextInt());
            val = Integer.toString(rand.nextInt());
            server.put(key, val);
            map.put(key, val);
        }
        Iterator<Map.Entry<String, String>> mapIter = map.entrySet().iterator();
        Map.Entry<String, String> pair;
        while(mapIter.hasNext()) {
            pair = mapIter.next();
            assertTrue(server.hasKey(pair.getKey()));
            assertEquals(pair.getValue(), server.get(pair.getKey()));
            mapIter.remove();
        }
        assertTrue(map.size() == 0);
    }

    @Test
    public void testNonexistentGetFails() {
        try {
            server.get("this key shouldn't be here");
            fail("get with nonexistent key should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_NO_SUCH_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void invalidInput0() {
        try {
            server.get("");
            fail("This should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_INVALID_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void invalidInput1() {
        try {
            server.get(null);
            fail("This should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_INVALID_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void invalidInput2() {
        try {
            server.put("", "blah");
            fail("This should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_INVALID_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void invalidInput3() {
        try {
            server.put(null, "blah");
            fail("This should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_INVALID_KEY, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void invalidInput4() {
        try {
            server.put("blah", "");
            fail("This should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_INVALID_VALUE, e.getKVMessage().getMessage());
        }
    }
    
    @Test
    public void invalidInput5() {
        try {
            server.put("blah", null);
            fail("This should error");
        } catch (KVException e) {
            assertEquals(KVConstants.RESP, e.getKVMessage().getMsgType());
            assertEquals(KVConstants.ERROR_INVALID_VALUE, e.getKVMessage().getMessage());
        }
    }
    


}
