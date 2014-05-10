package kvstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.ArrayList;

import org.junit.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TPCMasterTest {
	
	private static int TIMEDOUT = TPCMaster.TIMEOUT + 5;
	
	/**
	 * Used to inject a mocked socket of our choosing into the connection established in handleTPCRequest.
	 * @param mockSocket
	 * @return a modified TPCMaster
	 */
	private TPCMaster mockedSocketTPCMaster(final MockSocketeer mockSocketeer) {
		
		//Return a tpcMaster that has been modified to use partially mocked TPCSlaveInfo, that themselves return mocked sockets of our choosing
		TPCMaster tpcMaster = new TPCMaster(2, new KVCache(1, 4)) {
			
			@Override
			public TPCSlaveInfo findFirstReplica(String s) {
				TPCSlaveInfo mockSlaveInfo = mock(TPCSlaveInfo.class);
				try {
					when(mockSlaveInfo.connectHost(TIMEOUT)).thenReturn(mockSocketeer.getSocket(0))
															.thenReturn(mockSocketeer.getSocket(2));
				} catch (KVException kve) {
					kve.printStackTrace();
				}
				return mockSlaveInfo;
			}
			
			@Override
			public TPCSlaveInfo findSuccessor(TPCSlaveInfo t) {
				TPCSlaveInfo mockSlaveInfo = mock(TPCSlaveInfo.class);
				try {
					when(mockSlaveInfo.connectHost(TIMEOUT)).thenReturn(mockSocketeer.getSocket(1))
															.thenReturn(mockSocketeer.getSocket(3));
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
	private InputStream fakedResponseStream(String fakeResponse) throws KVException, UnsupportedEncodingException {
		KVMessage toSend = new KVMessage(fakeResponse);
		String fakedXML = toSend.toXML();
		return new ByteArrayInputStream(fakedXML.getBytes("UTF-8"));
	}
	
	/**
	 * Gets the XML string written to a mocked socket by a TPCMaster
	 * 
	 * @param masterEavesdropper - the ByteArrayOutputStream returned by a mocked socket
	 * @return an XML string of a KVMessage
	 * @throws UnsupportedEncodingException 
	 */
	private String eavesdroppedDecision(ByteArrayOutputStream masterEavesdropper) throws UnsupportedEncodingException {
		//return new String(masterEavesdropper.toByteArray(), "UTF-8");
		return masterEavesdropper.toString("UTF-8");
	}

	@Test
	public void registerSlaveTest() {
		TPCMaster temp = new TPCMaster(3, new KVCache(1,4)) {
			
			@Override
			public TPCSlaveInfo findFirstReplica(String key) {
		        // implement me
		    	if (key == null) {return null;}
		    	long keyID = -1;
		    	
		    	boolean x = TPCMaster.isLessThanEqualUnsigned(keyID, slaveList.get(0).getSlaveID());
		    	
		    	int n = 0;
		    	
		    	for (int i = 1; i < slaveList.size(); i++) {
		    		boolean y = TPCMaster.isLessThanEqualUnsigned(keyID, slaveList.get(i).getSlaveID());
		    		if (x != y) {
		    			n = i;
		    			break;
		    		}
		    	}
		    	
		        return slaveList.get(n);
		    }
		};
		
		TPCSlaveInfo slave0 = null, slave1 = null, slave2 = null, slave3 = null, slave4 = null;
		try {
			slave0 = new TPCSlaveInfo("40@hello1:5050");
			slave1 = new TPCSlaveInfo("10@hello2:5060");
			slave2 = new TPCSlaveInfo("30@hello3:5070");
			slave3 = new TPCSlaveInfo("20@hello4:5080");
		} catch (KVException e) {
			
		}
		
		if (slave0 == null || slave1 == null || slave2 == null || slave3 == null) {
			System.out.println("failed to construct slaves @registerSlaveTest");
			return;
		}
		
		temp.registerSlave(slave0); //40
		temp.registerSlave(slave1); //10
		temp.registerSlave(slave2); //30
		
		temp.registerSlave(slave3); //extra slave
		temp.registerSlave(slave4); //null slave, should do absolutely nothing
		
		//Test1 - registering invalid slaves
		if (temp.slaveList.size() != 3 && temp.slaveList.get(0).getSlaveID() == 10) {
			System.out.println("registerSlaveTest failed: Test1");
		} else {
			System.out.println("Test1 success");
		}
		
		//Test2 - findfirstreplica
		TPCSlaveInfo dummy = temp.findFirstReplica("dummy");
		//System.out.println(dummy.getSlaveID());
		if(dummy.getSlaveID() != 10) {
			System.out.println("registerSlaveTest failed: Test2");
		} else {
			System.out.println("Test2 success");
		}
		
		//Test3 - findsecondreplica
		TPCSlaveInfo dummy2 = temp.findSuccessor(dummy);
		//System.out.println(dummy2.getSlaveID());
		if(dummy2.getSlaveID() != 30) {
			System.out.println("registerSlaveTest failed: Test3");
		} else {
			System.out.println("Test3 success");
		}
		
		//Test4 - reregister a slave
		String temphostname = slave0.getHostname();
		int tempport = slave0.getPort();
		
		try {
			slave0 = new TPCSlaveInfo("40@hello1mod:5051");
		} catch (KVException e) {
			
		}
		
		//System.out.println(slave0.getHostname());
		//System.out.println(slave0.getPort());
		//System.out.println(temphostname);
		//System.out.println(tempport);
		
		if (slave0.getHostname().equals(temphostname) && tempport == slave0.getPort()) {
			System.out.println("registerSlaveTest failed: Test4");
		} else {
			System.out.println("Test4 success");
		}
	}
	
	/*
	@Test
	public void slaveTimesOutTestP1() {
		try {
			MockSocketeer mockSockets = new MockSocketeer();
			mockSockets.timeout(true, true);
			
			TPCMaster testMaster = mockedSocketTPCMaster(mockSockets);
			
			KVMessage fakedPut = new KVMessage(KVConstants.PUT_REQ);
			fakedPut.setKey("foo");
			fakedPut.setValue("bar");
			
			testMaster.handleTPCRequest(fakedPut, true);
			
			KVMessage abortDecision = new KVMessage(KVConstants.ABORT);
			String decisionXML = abortDecision.toXML();
			String eavesdroppedDecisionXML = eavesdroppedDecision(mockSockets.getEavesdropper(2));
			System.out.println(eavesdroppedDecisionXML);
			assertTrue(decisionXML.equals(eavesdroppedDecisionXML));
		} catch (KVException kve) {
			fail();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}
	*/
	
	/*
	 * Simulates a slave returning an abort vote by injecting mock sockets, 
	 * with faked response messages and eavesdropping outputstreams
	 */
	@Test
	public void slaveIndicatesFailureP1() {
		
		try {
			MockSocketeer mockSockets = new MockSocketeer(); 
			mockSockets.abortVote(true);
			
			TPCMaster testMaster = mockedSocketTPCMaster(mockSockets);
			//Test call - data should now be written to masterEavesdropper
			KVMessage fakedPut = new KVMessage(KVConstants.PUT_REQ);
			fakedPut.setKey("foo");
			fakedPut.setValue("bar");
			
			testMaster.handleTPCRequest(fakedPut, true);
			
			//Abort decision to compare against
			KVMessage abortDecision = new KVMessage(KVConstants.ABORT);
			String decisionXML = abortDecision.toXML();
			String eavesdroppedDecisionXML = eavesdroppedDecision(mockSockets.getEavesdropper(2));
			assertTrue(decisionXML.equals(eavesdroppedDecisionXML));
			
		} catch (KVException kve) {
			System.out.println("KVException in slaveIndicatesFailureP1: " + kve.getKVMessage().getMessage());
			kve.printStackTrace();
			fail();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}
	
	/* From the spec;
	 * "if the master receives anything besides an ACK [in phase 2], 
	 * throw a KVException ERROR_INVALID_FORMAT and return this to the client"
	 */
	@Test
	public void masterReceivesInvalidFormatP2() {

		try {
			MockSocketeer mockSockets = new MockSocketeer();
			Socket mockSocket = mock(Socket.class);
			//Should not return ready in phase 2
			when(mockSocket.getInputStream()).thenReturn(fakedResponseStream(KVConstants.READY));
			mockSockets.setSocket(2, mockSocket);
			
			TPCMaster testMaster = mockedSocketTPCMaster(mockSockets);
			//Test call - data should now be written to masterEavesdropper
			KVMessage fakedPut = new KVMessage(KVConstants.PUT_REQ);
			fakedPut.setKey("foo");
			fakedPut.setValue("bar");
			
			testMaster.handleTPCRequest(fakedPut, true);
			//Should not reach this point
			fail();
			
		} catch (KVException kve) {
			assertTrue(kve.getKVMessage().getMessage() == KVConstants.ERROR_INVALID_FORMAT);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}
	}
	
	//Essentially just a collection of the various (mocked) sockets used in handleTPCRequest
	private class MockSocketeer {
		
		private ArrayList<Socket> sockets;
		private ArrayList<ByteArrayOutputStream> outputStreams;	
		
		public MockSocketeer () throws IOException, KVException{
			//Instantiate mocked sockets and their respective output streams
			sockets = new ArrayList<Socket>(4);
			outputStreams = new ArrayList<ByteArrayOutputStream>(4);
			for(int i = 0; i < 4; i++) {
				Socket socket = mock(Socket.class);
				ByteArrayOutputStream stream = new ByteArrayOutputStream();
				sockets.add(i, socket);
				outputStreams.add(i, stream);
				when(socket.getOutputStream()).thenReturn(outputStreams.get(i));
			}
			//Instantiate a successful response series;
			when(sockets.get(0).getInputStream()).thenReturn(fakedResponseStream(KVConstants.READY)); //Replica P1
			when(sockets.get(1).getInputStream()).thenReturn(fakedResponseStream(KVConstants.READY)); //Successor P1
			when(sockets.get(2).getInputStream()).thenReturn(fakedResponseStream(KVConstants.ACK)); //Replica P2
			when(sockets.get(3).getInputStream()).thenReturn(fakedResponseStream(KVConstants.ACK)); //Successor P2
		}
		
		public void abortVote(boolean isReplica) throws UnsupportedEncodingException, IOException, KVException {
			//Overwrite either the replica or the successor with an abort socket
			int index = isReplica ? 0 : 1;
			Socket testSocket = mock(Socket.class);
			when(testSocket.getInputStream()).thenReturn(fakedResponseStream(KVConstants.ABORT));
			when(testSocket.getOutputStream()).thenReturn(outputStreams.get(index));
			sockets.set(index, testSocket);
		}
		
		public void timeout(boolean isReplica, boolean isPhase1) throws UnsupportedEncodingException, IOException, KVException, InterruptedException {
			int index = isReplica ? 0 : 1;
			index += isPhase1 ? 0 : 2;
			Socket testSocket = mock(Socket.class);
			when(testSocket.getInputStream()).thenReturn(delayStream(KVConstants.READY));
			when(testSocket.getOutputStream()).thenReturn(outputStreams.get(index));
			sockets.set(index, testSocket);
		}
		
		public void setSocket(int index, Socket mockSock) throws IOException {
			sockets.set(index, mockSock);
			when(mockSock.getOutputStream()).thenReturn(outputStreams.get(index));
		}
		
		private InputStream delayStream(String msg) throws UnsupportedEncodingException, KVException, InterruptedException {
			Thread.sleep(TIMEDOUT);
			return fakedResponseStream(msg);
		}
		
		public Socket getSocket(int index) {
			return sockets.get(index);
		}
		
		public ByteArrayOutputStream getEavesdropper(int index) {
			return outputStreams.get(index);
		}
	}
	
	//thenCallRealMethod()
}
