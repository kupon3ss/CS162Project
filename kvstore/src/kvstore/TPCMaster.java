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
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered.Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        // implement me
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
        return null;
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        // implement me
        return null;
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
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
                                throws KVException {
        // implement me
        /*String key = msg.getKey();
        KVServer.checkKey(key); // pass exception on to caller

        Lock setLock = masterCache.getLock(key);
        setLock.lock();

        try {
            TPCSlaveInfo firstSlave = findFirstReplica(key);
            TPCSlaveInfo secondSlave = findSuccessor(firstSlave);

            Socket firstSocket = connectSlave(firstSlave);
            Socket secondSocket = connectSlave(secondSlave);

            msg.sendMessage(firstSocket);
            KVMessage phase1Response = new KVMessage(firstSocket, TIMEOUT);
            if (phase1Response.getMsgType().equals(READY))
                // do nothing
            else if (phase1Response.getMsgType().equals(ABORT))
                ; // global-abort, abort

            msg.sendMessage(secondSocket);
            KVMessage phase2Response = new KVMessage(secondSocket, TIMEOUT);
            if (secondResponse.getMsgType().equals(READY))
                ;// ready, commit
            else if (secondResponse.getMsgType().equals(ABORT))
                ; // global-abort, abort

            if type.equals(response)
            //assert response.getKey().equals(msg.getKey());
            return response.getValue();

        } finally {
            setLock.unlock();
        }*/
    }

    /*private void tpcPhase1(KVMessage msg) throws KVException {
        String key = msg.getKey();
    }

    private void tpcPhase2() {

    }*/

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
     *         the value from either slave for any reason
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

            TPCSlaveInfo replica = findFirstReplica(key);
            try {
                // attempt to get from first replica
                val = getFromSlave(msg, replica);
            } catch (KVException kve) {
                // retry with second, which may throw an excepion (not handled here, passed to caller)
                replica = findSuccessor(replica);
                val = getFromSlave(msg, replica);
            }
            masterCache.put(key, val);
            return val;

        } finally {
            lock.unlock();
        }
    }

    private String getFromSlave(KVMessage msg, TPCSlaveInfo replica) throws KVException {
        try {
            Socket slaveSocket = connectSlave(replica);
            msg.sendMessage(slaveSocket);
            KVMessage response = new KVMessage(slaveSocket);
            //assert response.getKey().equals(msg.getKey());
            return response.getValue();
        } catch (KVException kve) {
            throw new KVException(ERROR_NO_SUCH_KEY);
        }
    }

    /**
     * Creates a socket connected to the slave to make a request.
     *
     * @return Socket connected to slave server
     * @throws KVException if unable to make or connect socket
     */
    private Socket connectSlave(TPCSlaveInfo slaveInfo) throws KVException {
        try {
        	return new Socket(slaveInfo.getHostname(), slaveInfo.getPort());
        } catch (UnknownHostException uhe) {
        	throw new KVException(ERROR_COULD_NOT_CONNECT);
        } catch (IOException ioe) {
        	throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        }
    }

}
