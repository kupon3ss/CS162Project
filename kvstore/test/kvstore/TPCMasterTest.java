package kvstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import org.junit.*;

import static org.mockito.Mockito.*;

public class TPCMasterTest {
	
	private static int TIMEDOUT = TPCMaster.TIMEOUT + 5;
	
	/**
	 * Used to inject a mocked socket of our choosing into the connection established in handleTPCRequest.
	 * @param mockSocket
	 * @return a modified TPCMaster
	 */
	private TPCMaster mockedSocketTPCMaster(final Socket mockSocket) {
		
		//Return a tpcMaster that has been modified to use partially mocked TPCSlaveInfo, that themselves return mocked sockets of our choosing
		TPCMaster tpcMaster = new TPCMaster(2, new KVCache(1, 4)) {
			
			@Override
			public TPCSlaveInfo findFirstReplica(String s) {
				TPCSlaveInfo mockSlaveInfo = mock(TPCSlaveInfo.class);
				try {
					when(mockSlaveInfo.connectHost(TIMEOUT)).thenReturn(mockSocket);
				} catch (KVException kve) {
					kve.printStackTrace();
				}
				return mockSlaveInfo;
			}
			
			@Override
			public TPCSlaveInfo findSuccessor(TPCSlaveInfo t) {
				TPCSlaveInfo mockSlaveInfo = mock(TPCSlaveInfo.class);
				try {
					when(mockSlaveInfo.connectHost(TIMEOUT)).thenReturn(mockSocket);
				} catch (KVException kve) {
					kve.printStackTrace();
				}
				return mockSlaveInfo;
			}
		};
		
		return tpcMaster;
	}
	
	/**
	 * Use to fake an InputStream for a mocked socket, to fake messages from an imaginary slave;
	 * See: http://stackoverflow.com/questions/782178/how-do-i-convert-a-string-to-an-inputstream-in-java
	 * 
	 * @param fakeResponse - One of KVMessage.SET_TYPES
	 * @return an InputStream from the XML of the KVMessage of our choosing
	 * @throws KVException
	 * @throws UnsupportedEncodingException
	 */
	private InputStream fakedResponse(String fakeResponse) throws KVException, UnsupportedEncodingException {
		KVMessage toSend = new KVMessage(fakeResponse);
		String fakedXML = toSend.toXML();
		return new ByteArrayInputStream(fakedXML.getBytes("UTF-8"));
	}
	
	/**
	 * Gets the XML string written to a mocked socket by a TPCMaster
	 * 
	 * @param masterEavesdropper - the ByteArrayOutputStream returned by a mocked socket
	 * @return an XML string of a KVMessage
	 */
	private String eavesdroppedDecision(ByteArrayOutputStream masterEavesdropper) {
		return "";
	}

	@Test
	public void slaveTimesOutTestP1() {
		
	}
	
	@Test
	public void slaveIndicatesFailureP1() {
		
	}
	
	@Test
	public void slaveTimesOutP2() {
		
	}
	
	@Test
	public void slaveIndicatesFailureP2() {
		
	}
	
	/* From the spec;
	 * "if the master receives anything besides an ACK [in phase 2], 
	 * throw a KVException ERROR_INVALID_FORMAT and return this to the client"
	 */
	@Test
	public void masterReceivesInvalidFormatP2() {
		
	}
	
	//thenCallRealMethod()
}
