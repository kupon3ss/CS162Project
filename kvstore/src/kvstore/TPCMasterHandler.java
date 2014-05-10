package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    private long slaveID;
    private KVServer kvServer;
    private TPCLog tpcLog;
    private ThreadPool threadpool;

    private boolean tpcOperationInProgress;
    
    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
        this.tpcOperationInProgress = false;
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server) throws KVException {
    	Socket master = null;
    	try {
	        KVMessage registerSlave = new KVMessage(REGISTER, slaveID+"@"+server.getHostname()+":"+server.getPort());
	        master = new Socket(masterHostname, 9090);
	        registerSlave.sendMessage(master);
	        
	        KVMessage response = new KVMessage(master);
	        if (!response.getMessage().equals(SUCCESS)) {
	        	throw new KVException(ERROR_INVALID_FORMAT);
	        }
    	} catch (IOException e) {
    		throw new KVException(ERROR_INVALID_FORMAT);
    	} finally {
    		try {
    			master.close();
    		} catch (IOException|NullPointerException ioe) {
    			//Best effort to close
    		}
    	}
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        Runnable r = new MasterHandler(master);
        threadpool.addJob(r);
    }

    private KVMessage handleAbort(KVMessage request){
		String lastMessageType = tpcLog.getLastEntry().getMsgType();
		if (lastMessageType.equals(DEL_REQ) || lastMessageType.equals(PUT_REQ))
	        tpcLog.appendAndFlush(request);
        tpcOperationInProgress = false;
		return new KVMessage(ACK);
	}

	private KVMessage handleCommit(KVMessage request) {
        if (tpcOperationInProgress) {
            KVMessage lastMessage = tpcLog.getLastEntry();
            try {
                if (lastMessage.getMsgType().equals(DEL_REQ)) {
                    kvServer.del(lastMessage.getKey());
                } else if (lastMessage.getMsgType().equals(PUT_REQ)) {
                    kvServer.put(lastMessage.getKey(), lastMessage.getValue());
                }
            } catch (KVException kve) {
                // ignore errors (these were already checked on master and ERROR_NO_SUCH_KEY checked for del in phase 1)
            }
            tpcLog.appendAndFlush(request);
        }
        // always send a response
        tpcOperationInProgress = false;
        return new KVMessage(ACK);
	}

	private KVMessage handlePutReq(KVMessage request) {
        // phase-1 only
        tpcLog.appendAndFlush(request);
        tpcOperationInProgress = true;
        return new KVMessage(READY);
    }

    private KVMessage handleDelReq(KVMessage request) {
        String key = request.getKey();
        // phase 1 only
        if (!kvServer.hasKey(key)) // this check is only made in phase 1; thus we can ignore the exception in phase 2
            return new KVMessage(ABORT, ERROR_NO_SUCH_KEY);
        tpcLog.appendAndFlush(request);
        tpcOperationInProgress = true;
        return new KVMessage(READY);
    }

    private KVMessage handleGet(KVMessage request) {
        String key = request.getKey();
        KVMessage response = new KVMessage(RESP);
		try {
            response.setKey(key);
			response.setValue(kvServer.get(key));
		} catch (KVException e) {
            response.setMessage(e.getMessage());
        }
        return response;
	}

    /**
     * Runnable class containing routine to service a message from the master.
     */
    private class MasterHandler implements Runnable {

        private Socket master;

        /**
         * Construct a MasterHandler.
         *
         * @param master Socket connected to master with the message
         */
        public MasterHandler(Socket master) {
            this.master = master;
        }

        /**
         * Processes request from master and sends back a response with the
         * result. This method needs to handle both phase1 and phase2 messages
         * from the master. The delivery of the response is best-effort. If
         * we are unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
        	KVMessage response, request;
        	try {
                request = new KVMessage(master);
            } catch (KVException kve) {
                return;
            }
            switch (request.getMsgType()) {
                case GET_REQ:
                    response = handleGet(request);
                    break;
                case PUT_REQ:
                    response = handlePutReq(request);
                    break;
                case DEL_REQ:
                    response = handleDelReq(request);
                    break;
                case COMMIT:
                    response = handleCommit(request);
                    break;
                case ABORT:
                    response = handleAbort(request);
                    break;
                default: // should never happen, but in case the master were to send some other messag
                    return;
            }
    		try {
    			response.sendMessage(master);
    		} catch (KVException e) {
				e.printStackTrace(); //best-effort response; can't do anything if we can't reach the master
			}
        }
    }
}