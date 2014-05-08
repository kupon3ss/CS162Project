package kvstore;

import java.net.Socket;

import org.junit.*;

import static org.mockito.Mockito.*;

public class TPCMasterTest {
	
	private static int TIMEDOUT = TPCMaster.TIMEOUT + 5;
	
	private TPCMaster mockedSocketTPCMaster(final Socket mockSocket) {
		
		//Return a tpcMaster that has been modified to use partially mocked TPCSlaveInfo, that themselves operate on mocked sockets of our choosing
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
	
	@Test
	public void registerSlaveTest() {
		TPCMaster temp = new TPCMaster(3, new KVCache(1,4));
		TPCSlaveInfo slave0 = null, slave1 = null, slave2 = null, slave3 = null;
		try {
			slave0 = new TPCSlaveInfo("10@hello:5050");
			slave1 = new TPCSlaveInfo("20@hello:5060");
			slave2 = new TPCSlaveInfo("30@hello:5070");
			slave3 = new TPCSlaveInfo("40@hello:5080");
		} catch (KVException e) {
			
		}
		
		if (slave0 == null || slave1 == null || slave2 == null || slave3 == null) {
			System.out.println("failed to construct slaves @registerSlaveTest");
			return;
		}
		
		temp.registerSlave(slave0);
		temp.registerSlave(slave1);
		temp.registerSlave(slave2);
		temp.registerSlave(slave3);
		
		//Test1
		if (temp.slaveList.size() != 3) {
			System.out.println("registerSlaveTest failed: Test1");
		}
		
		System.out.println("Success @registerSlaveTest");
	}
	
	@Test
	public void findTest() {
		
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
