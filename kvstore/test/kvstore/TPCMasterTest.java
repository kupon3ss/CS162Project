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
			slave0 = new TPCSlaveInfo("40@hello:5050");
			slave1 = new TPCSlaveInfo("10@hello:5060");
			slave2 = new TPCSlaveInfo("30@hello:5070");
			slave3 = new TPCSlaveInfo("20@hello:5080");
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
		
		//Test1
		if (temp.slaveList.size() != 3) {
			System.out.println("registerSlaveTest failed: Test1");
		} else {
			System.out.println("Test1 success");
		}
		
		//Test2
		TPCSlaveInfo dummy = temp.findFirstReplica("dummy");
		System.out.println(dummy.getSlaveID());
		if(temp.findFirstReplica("dummy").getSlaveID() != 10) {
			System.out.println("registerSlaveTest failed: Test2");
		} else {
			System.out.println("Test2 success");
		}
		
		//Test3
		TPCSlaveInfo dummy2 = temp.findSuccessor(dummy);
		System.out.println(dummy2.getSlaveID());
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
