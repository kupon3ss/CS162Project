package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class KVClientTest {
	
	private Socket socketMock;
	
	@Before
	private KVClient clientMockSocketSetup() {
		
		try {
			String hostname = InetAddress.getLocalHost().getHostAddress();
	       	KVClient client = new KVClient(hostname, 8080) {
	       		@Override
	       		protected Socket connectHost() throws KVException {
	       			Socket sock = mock(Socket.class);
	       			return sock;
	       		}
	       	};
	       	return client;
		} catch (IOException e){
			System.out.println("Inet setup failed in clientMockSocketSetup");
		}
		return null;
	}

    @Test(timeout = 20000)
    public void testInvalidKey() throws IOException {
    	
    	KVClient client = clientMockSocketSetup();
    	
        try {
            client.put("", "bar");
            fail("Didn't fail on null value");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_KEY);
        }
    }
    
    @Test(timeout = 20000)
    public void testInvalidValue() {
    	
    }
    
    //Basic put/get cycle
    @Test(timeout = 20000)
    public void testRoundTrip() {
    	
    }
    
    //invoke a 'could not receive data' error
    @Test(timeout = 20000)
    public void testBadData() {
    	
    }
    
    //invoke a 'could not send data' error
    @Test(timeout = 20000)
    public void testBadSend() {
    	
    }
    
    //invoke a 'could not create socket' error
    @Test(timeout = 20000)
    public void testBadSocket() {
    	
    }
    
    //invoke a 'could not connect' error
    @Test(timeout = 20000)
    public void testBadConnect() {
    	
    }
    
    

}
