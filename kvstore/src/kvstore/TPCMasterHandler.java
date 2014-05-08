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
    private int phase;
    
    private String masterHostname;
    private SocketServer ss;

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
    	this.masterHostname = masterHostname;
    	this.ss = server;
    	Socket master = null;
    	try {
	        KVMessage registerSlave = new KVMessage(REGISTER, slaveID+"@"+server.getHostname()+":"+server.getPort());
	        master = new Socket(masterHostname, 9090);
	        registerSlave.sendMessage(master);
	        
	        KVMessage response = new KVMessage(master);
	        if (response.getMsgType() != SUCCESS) {
	        	throw new KVException(ERROR_INVALID_FORMAT);
	        }
    	} catch (IOException e) {
    		throw new KVException(ERROR_INVALID_FORMAT);
    	} finally {
    		try {
    			master.close();
    		} catch (IOException ioe) {
    			//Best effort to close
    		}
    	}
    }

    private void reRegister() {
        try {
            tpcLog.rebuildServer();
            registerWithMaster(masterHostname, ss);
        } catch (KVException kve) {}
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
        	phase = 0;
        	KVMessage message = null;
        	KVMessage request = null;
        	KVMessage response = null;
        	try {
        		request = new KVMessage(master);
                switch (request.getMsgType()) {
                	case GET_REQ:
                		
                		response = handleGet(request);
                		break;
                	case PUT_REQ:
                		
                		response = handlePut(request);
                		break;
                		
                	case DEL_REQ:
                		
                		response = handleDel(request);
                		break;
                		
                    case COMMIT:
                    	
                		response = handleCommit(request);
                        break;
                        
                    case ABORT:
                    	
                		response = handleAbort(request);
                    	break;
                    
                    default: // should never happen, but in case a client were to send some other message
                        response = new KVMessage(RESP, ERROR_INVALID_REQUEST);
                }
        	} catch (KVException kve) {
        		response = kve.getKVMessage();
        	}

    		try {
    			response.sendMessage(master);
    		} catch (KVException e) {
                KVMessage ErrorResponse = e.getKVMessage();
                try {
					ErrorResponse.sendMessage(master);
				} catch (KVException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    		}
        	
        	
        	
        	tpcLog.appendAndFlush(message);
        }

		private KVMessage handleAbort(KVMessage request){
			// TODO Auto-generated method stub
			try {
				KVMessage lastMessage = tpcLog.getLastEntry();
				if (lastMessage.getMsgType() == DEL_REQ || lastMessage.getMsgType() == PUT_REQ){
		            tpcLog.appendAndFlush(request);
		            phase = 0;
				}
				KVMessage response = new KVMessage(ACK);
				return response;
				
			} catch (KVException e) {
                KVMessage ErrorResponse = e.getKVMessage();
                return ErrorResponse;
			}

		}

		private KVMessage handleCommit(KVMessage request) throws KVException {
			// TODO Auto-generated method stub
			// state2 is if commit was received but action has not made yet
			if(phase == 1 || phase == 2){
				phase = 2;
				KVMessage lastMessage = tpcLog.getLastEntry();
				if (lastMessage.getMsgType() == DEL_REQ){
					handleDel(lastMessage);
				}
				else if (lastMessage.getMsgType() == PUT_REQ){
					handlePut(lastMessage);
				}				
				tpcLog.appendAndFlush(request);
			try {
				KVMessage response = new KVMessage(ACK);
				return response;
			} catch (KVException e) {
                KVMessage ErrorResponse = new KVMessage(ABORT);
                ErrorResponse.setMessage(e.getKVMessage().getMessage());
                return ErrorResponse;
			}
			
			}
			
			if(phase == 0){
			try {
				KVMessage response = new KVMessage(ACK);
				return response;
			} catch (KVException e) {
                KVMessage ErrorResponse = new KVMessage(ABORT);
                ErrorResponse.setMessage(e.getKVMessage().getMessage());
                return ErrorResponse;
			}
		}
			//should never get here
			return request;
		}

		private KVMessage handlePut(KVMessage request) {//throws KVException {
			// TODO Auto-generated method stub
			if(phase == 0){
				KVServer.checkKey(request.getKey());
				tpcLog.appendAndFlush(request);
				phase ++;
				try {
					KVMessage response = new KVMessage(READY);
					return response;
				} catch (KVException e) {
	                KVMessage ErrorResponse = new KVMessage(ABORT);
	                ErrorResponse.setMessage(e.getKVMessage().getMessage());
					return ErrorResponse;

				}
			}
			
			else if (phase == 2){
		        try {
		        	KVServer.checkKey(request.getKey());
		            kvServer.put(request.getKey(), request.getValue());
		            phase = 0;
		            KVMessage response = new KVMessage(ACK);
		            return response;
		        } catch (KVException e) {
	                KVMessage ErrorResponse = new KVMessage(ABORT);
	                ErrorResponse.setMessage(e.getKVMessage().getMessage());
					return ErrorResponse;
					}
		            	
		            }
			//should never get here
			return request;
		}

		private KVMessage handleGet(KVMessage request) {
			// TODO Auto-generated method stub
				if (kvServer.hasKey(request.getKey())){
				try {
					KVServer.checkKey(request.getKey());
					KVMessage response = new KVMessage(RESP);
					response.setKey(request.getKey());
					response.setValue(kvServer.get(request.getKey()));
					return response;
				} catch (KVException e) {
	                KVMessage ErrorResponse = e.getKVMessage();
	                return ErrorResponse;
				}
			}
				else {
					KVMessage response = new KVMessage(RESP,"Key not found Error");
					return response;
					}

				}
			

		private KVMessage handleDel(KVMessage request) throws KVException {
			// TODO Auto-generated method stub
			if(phase == 0){
				KVServer.checkKey(request.getKey());
				tpcLog.appendAndFlush(request);
				phase ++;
				try {
					KVMessage response = new KVMessage(READY);
					return response;
				} catch (KVException e) {
	                KVMessage ErrorResponse = new KVMessage(ABORT);
	                ErrorResponse.setMessage(e.getKVMessage().getMessage());
					return ErrorResponse;
				}

			}
			
			else if (phase == 2){
		        try {
		        	KVServer.checkKey(request.getKey());
		            kvServer.del(request.getKey());
		            phase = 0;
		            KVMessage response = new KVMessage(ACK);
		            return response;
		        } catch (KVException e) {
	                KVMessage ErrorResponse = new KVMessage(ABORT);
	                ErrorResponse.setMessage(e.getKVMessage().getMessage());
						return ErrorResponse;
		            }
			}
			//should never get here;
			return request;

			
		}

    }
}