package kvstore;

import static org.junit.Assert.*;

import org.junit.*;

public class TPCSlaveInfoTest {

    /**
     * Verify slaveInfo can parse correctly.
     */
    @Test
    public void slaveInfoInput0() {
        try {
        	TPCSlaveInfo test0 = new TPCSlaveInfo("10@hello:50");
        	assertEquals(test0.getSlaveID(), 10);
        	assertEquals(test0.getPort(), 50);
        	assertEquals(test0.getHostname(), "hello");
        	
        	TPCSlaveInfo test1 = new TPCSlaveInfo("-10@world:50");
        	assertEquals(test1.getSlaveID(), -10);
        	assertEquals(test1.getPort(), 50);
        	assertEquals(test1.getHostname(), "world");
        	
        	TPCSlaveInfo test2 = new TPCSlaveInfo("1384095@foobar:50");
        	assertEquals(test2.getSlaveID(), 1384095);
        	assertEquals(test2.getPort(), 50);
        	assertEquals(test2.getHostname(), "foobar");
        	
        } catch (KVException kve) {
        	fail("This should not error");
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput1() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("asdf@hello:50");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput2() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("10@hello:asdf");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput3() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("10@he@llo:50");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput4() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("10@he:llo:50");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput5() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("@hello:50");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput6() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("10@hello:");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput7() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("10@:50");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
    
    /**
     * Bulletproof check.
     */
    @Test
    public void slaveInfoInput8() {
    	try {
        	TPCSlaveInfo test = new TPCSlaveInfo("10@hello:-50");
        	
        	fail("This should error");
        	
        } catch (KVException kve) {
        	assertEquals(KVConstants.RESP, kve.getKVMessage().getMsgType());
        	assertEquals(KVConstants.ERROR_INVALID_FORMAT, kve.getKVMessage().getMessage());
        }
    }
	
}
