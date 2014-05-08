package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.locks.Lock;

public class TPCMaster {

    private int numSlaves;
    private KVCache masterCache;
    
    public ArrayList<TPCSlaveInfo> slaveList;

    public static final int TIMEOUT = 3000;

    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        // implement me
        slaveList = new ArrayList<TPCSlaveInfo>();
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered.Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public synchronized void registerSlave(TPCSlaveInfo slave) {
        // implement me
    	if (slave == null) {return;}
    	
    	boolean reregister = false;
    	for (int i = 0; i < slaveList.size(); i++) {
    		if (slave.getSlaveID() == slaveList.get(i).getSlaveID()) {
    			reregister = true;
    		}
    	}
    	
    	if (slaveList.size() >= numSlaves && !reregister) {return;}
    	
    	if (!reregister) {
	    	int n = 0;
	    	for (int i = 0; i < slaveList.size(); i++) {
	    		if (TPCMaster.isLessThanEqualUnsigned(slaveList.get(i).getSlaveID(), slave.getSlaveID())) {
	    			n = i + 1;
	    		}
	    	}
	    	slaveList.add(n, slave);
    	} else {
    		for (int i = 0; i < slaveList.size(); i++) {
	    		if (slaveList.get(i).getSlaveID() == slave.getSlaveID()) {
	    			slaveList.set(i, slave);
	    		}
	    	}
    	}
    }

    public boolean ready() {
    	if (slaveList.size() == numSlaves) {
    		return true;
    	}
    	return false;
    }
    
    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
        // implement me
    	if (key == null) {return null;}
    	long keyID = TPCMaster.hashTo64bit(key);
    	
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

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        // implement me
        if (firstReplica == null) {return null;}
        
        int n = -1;
        for (int i = 0; i < slaveList.size(); i++) {
    		if (slaveList.get(i).getSlaveID() == firstReplica.getSlaveID()) {
    			if (i != slaveList.size() - 1) {
    				n = i + 1;
    			} else {
    				n = 0;
    			}
    		}
    	}
        
        if (n == -1) {return null;}
        return slaveList.get(n);
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq) throws KVException {
        // implement me
        String key = msg.getKey();
        KVServer.checkKey(key); // pass exception on to caller

        // lock the master cache set for this key to block GET requests
        Lock setLock = masterCache.getLock(key);
        setLock.lock();

        TPCSlaveInfo firstSlave = findFirstReplica(key);
        TPCSlaveInfo secondSlave = findSuccessor(firstSlave);
        Socket firstSocket = null, secondSocket = null;
        try {
            firstSocket = firstSlave.connectHost(TIMEOUT);
            secondSocket = secondSlave.connectHost(TIMEOUT);

            // phase-1
            String firstResponse = doTPCPhase1(msg, firstSocket);
            String secondResponse = doTPCPhase1(msg, secondSocket);
            boolean ready = firstResponse.equals(READY) && secondResponse.equals(READY);

            // phase-2
            msg = new KVMessage(ready ? COMMIT : ABORT);
            doTPCPhase2(msg, firstSocket);
            doTPCPhase2(msg, secondSocket);

            // put/del from master cache upon success
            if (isPutReq) masterCache.put(key, msg.getValue());
            else masterCache.del(key);

        } finally {
            setLock.unlock();
            if (firstSocket != null) firstSlave.closeHost(firstSocket);
            if (secondSocket != null) secondSlave.closeHost(secondSocket);
        }
    }

    private String doTPCPhase1(KVMessage msg, Socket slaveSocket) throws KVException {
        msg.sendMessage(slaveSocket);
        try {
            msg = new KVMessage(slaveSocket, TIMEOUT);
            return msg.getMsgType();
        } catch (KVException kve) { //timeout or other error
            return ABORT;
        }
    }

    private void doTPCPhase2(KVMessage msg, Socket slaveSocket) throws KVException {
        boolean sent = false;
        while (!sent) {
            msg.sendMessage(slaveSocket);
            try {
                msg = new KVMessage(slaveSocket, TIMEOUT);
                sent = true;
            } catch (KVException kve) {
                //timeout, repeat loop
            }
        }
        if (!msg.getMsgType().equals(ACK))
            throw new KVException(ERROR_INVALID_FORMAT);
    }

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVException from second replica
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from both slaves for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
        // implement me
        String key = msg.getKey();
        KVServer.checkKey(key); // pass exception on to caller

        // get the lock for this set and acquire (lock) it. any keys in the set protected by the
        //  lock cannot be accessed or updated while this critical section executes
        Lock lock = masterCache.getLock(key);
        lock.lock();

        try {
            // attempt to get from the master cache
            String val = masterCache.get(key);
            if (val != null) return val;
            // attempt to get from the replicas
            val = doGet(msg);
            masterCache.put(key, val);
            return val;
        } finally {
            lock.unlock();
        }
    }

    private String doGet(KVMessage msg) throws KVException {
        TPCSlaveInfo slave = findFirstReplica(msg.getKey());
        try { // attempt to get first replica
            return getFromSlave(msg, slave);
        } catch (KVException firstReplicaNoSuccess) {
            try { // retry with second
                return getFromSlave(msg, findSuccessor(slave));
            } catch (KVException noSuccess) {
                throw new KVException(ERROR_NO_SUCH_KEY);
            }
        }
    }

    private String getFromSlave(KVMessage msg, TPCSlaveInfo slave) throws KVException {
        Socket slaveSocket = null;
        try {
            slaveSocket = slave.connectHost(TIMEOUT);
            msg.sendMessage(slaveSocket);
            KVMessage response = new KVMessage(slaveSocket);
            //assert response.getKey().equals(msg.getKey());
            return response.getValue();
        } finally {
            if (slaveSocket != null) slave.closeHost(slaveSocket);
        }
    }

}
