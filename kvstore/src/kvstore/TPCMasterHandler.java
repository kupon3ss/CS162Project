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
    private int Handshakestate;

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
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
    	try {
	        KVMessage registerSlave = new KVMessage(REGISTER, slaveID+"@"+server.getHostname()+":"+server.getPort());
	        Socket master = new Socket(masterHostname, 9090);
	        registerSlave.sendMessage(master);
	        
	        KVMessage response = new KVMessage(master);
	        if (response.getMsgType() != SUCCESS) {
	        	throw new KVException(ERROR_INVALID_FORMAT);
	        }
    	} catch (IOException e) {
    		
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
        // implement me
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
        	Handshakestate = 0;
        	KVMessage message = null;
        	KVMessage request = null;
        	KVMessage response = null;
        	try {
        		request = new KVMessage(master);
                switch (request.getMsgType()) {
                	case GET_REQ:
                		
                		tpcLog.appendAndFlush(request);
                		break;
                	case PUT_REQ:
                		
                		tpcLog.appendAndFlush(request);
                		break;
                		
                	case DEL_REQ:
                		tpcLog.appendAndFlush(request);
                		break;
                		
                	case RESP:
                		
                		tpcLog.appendAndFlush(request);
                		break;
                		
                    case READY:
                        
                		tpcLog.appendAndFlush(request);
                    	response = new KVMessage(RESP);
                        break;
                        
                    case COMMIT:
                    	
                		tpcLog.appendAndFlush(request);
                        break;
                        
                    case ABORT:
                    	
                		tpcLog.appendAndFlush(request);

                    	break;
                    	
                    case ACK:
                		tpcLog.appendAndFlush(request);

                    	break;
                    
                    default: // should never happen, but in case a client were to send some other message
                        response = new KVMessage(RESP, ERROR_INVALID_REQUEST);
                }
        	} catch (KVException kve) {
        		response = kve.getKVMessage();
        	}
    		try {
    			response.sendMessage(master);
    		} catch (KVException kve) {
    			//best effort response (can't do anything)
    		}
        	
        	
        	
        	tpcLog.appendAndFlush(message);
        }

    }

}
