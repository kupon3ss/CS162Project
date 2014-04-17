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
	
	//private Socket socketMock;
	private String hostname;
	
	public KVClientTest() {
		try {
			hostname = InetAddress.getLocalHost().getHostAddress();
		} catch (IOException e) {
			System.out.println("Inet setup failed in KVClientTest");
		}
	}
	
	@Before
	private KVClient clientMockSocketSetup() {
		//String hostname = InetAddress.getLocalHost().getHostAddress();
       	KVClient client = new KVClient(hostname, 8080) {
       		@Override
       		protected Socket connectHost() throws KVException {
       			Socket mockedSocket = mock(Socket.class);
       			return mockedSocket;
       		}
       	};
       	return client;
	}

    @Test(timeout = 20000)
    public void testInvalidKey() throws IOException {
    	KVClient client = clientMockSocketSetup();
        try {
            client.put("", "bar");
            fail("Didn't fail on empty key");
        } catch (KVException kve) {
            String errorMsg = kve.getKVMessage().getMessage();
            assertEquals(errorMsg, ERROR_INVALID_KEY);
        }
    }
    
    @Test(timeout = 20000)
    public void testInvalidValue() {
    	KVClient client = clientMockSocketSetup();
    	try {
    		client.put("foo", "");
    		fail("Didn't fail on empty value");
    	} catch (KVException kve) {
    		String errorMsg = kve.getKVMessage().getMessage();
    		assertEquals(errorMsg, ERROR_INVALID_VALUE);
    	}
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
    	KVClient client = new KVClient(hostname, 8080) {
    		//Stub a faulty socket method
    		@Override
    		protected Socket connectHost() throws KVException {
    			try {
    				//Simulate a failed socket setup
    				throw new IOException();
    			} catch (UnknownHostException uhe) {
    				throw new KVException(ERROR_COULD_NOT_CONNECT);
    			} catch (IOException ioe) {
    				throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
    			}
    		}
    	};
    	
    	try {
    		client.put("foo", "bar");
    		fail("Didn't fail on forced bad socket");
    	} catch (KVException kve) {
    		String errorMsg = kve.getKVMessage().getMessage();
    		assertEquals(errorMsg, ERROR_COULD_NOT_CREATE_SOCKET);
    	}
    }
    
    //invoke a 'could not connect' error - very similar to testBadSocket, see: https://piazza.com/class/hn5b1n5g5rk1oc?cid=817
    @Test(timeout = 20000)
    public void testBadConnect() {
    	KVClient client = new KVClient(hostname, 8080) {
    		//Stub a faulty socket method
    		@Override
    		protected Socket connectHost() throws KVException {
    			try {
    				//Simulate a failed socket connection
    				throw new UnknownHostException();
    			} catch (UnknownHostException uhe) {
    				throw new KVException(ERROR_COULD_NOT_CONNECT);
    			} catch (IOException ioe) {
    				throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
    			}
    		}
    	};
    	
    	try {
    		client.put("foo", "bar");
    		fail("Didn't fail on forced bad connect");
    	} catch (KVException kve) {
    		String errorMsg = kve.getKVMessage().getMessage();
    		assertEquals(errorMsg, ERROR_COULD_NOT_CONNECT);
    	}
    }
    
    

}
