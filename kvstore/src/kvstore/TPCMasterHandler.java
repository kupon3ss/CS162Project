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
    		throw new KVException(ERROR_INVALID_FORMAT);
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
        	} catch (KVException kve) {
        		response = kve.getKVMessage();
        	}
                switch (request.getMsgType()) {
                	case GET_REQ:
                		
                		handleGet(request);
                		break;
                	case PUT_REQ:
                		
                		handlePut(request);
                		break;
                		
                	case DEL_REQ:
                		handleDel(request);
                		break;
                		
                    case COMMIT:
                    	
                		handleCommit(request);
                        break;
                        
                    case ABORT:
                    	
                		handleAbort(request);
                    	break;
                    
                    default: // should never happen, but in case a client were to send some other message
                        response = new KVMessage(RESP, ERROR_INVALID_REQUEST);
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

		private void handleAbort(KVMessage request) {
			// TODO Auto-generated method stub
			try {
				KVMessage response = new KVMessage("abort");
				response.sendMessage(master);
	            tpcLog.appendAndFlush(request);
			} catch (KVException e) {
                KVMessage ErrorResponse = e.getKVMessage();
                try {
					ErrorResponse.sendMessage(master);
				} catch (KVException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}

		private void handleCommit(KVMessage request) {
			// TODO Auto-generated method stub
			// state2 is if commit was received but action has not made yet
			if(Handshakestate == 1 || Handshakestate == 2){
				Handshakestate = 2;
				KVMessage lastMessage = tpcLog.getLastEntry();
				if (lastMessage.getMsgType() == DEL_REQ){
					handleDel(lastMessage);
				}
				else if (lastMessage.getMsgType() == DEL_REQ){
					handlePut(lastMessage);
				}				
				tpcLog.appendAndFlush(request);
			try {
				KVMessage response = new KVMessage("ack");
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
			}
			
			if(Handshakestate == 0){
			try {
				KVMessage response = new KVMessage("ack");
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
			}
		}

		private void handlePut(KVMessage request) {
			// TODO Auto-generated method stub
			if(Handshakestate == 0){
				try {
					KVMessage response = new KVMessage("ready");
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
				tpcLog.appendAndFlush(request);
				Handshakestate ++;
			}
			
			else if (Handshakestate == 2){
		        try {
		            kvServer.put(request.getKey(), request.getValue());
		            KVMessage response = new KVMessage("ack");
		            response.sendMessage(master);
		            Handshakestate = 0;
		        } catch (KVException e) {
	                KVMessage ErrorResponse = e.getKVMessage();
	                try {
						ErrorResponse.sendMessage(master);
					} catch (KVException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		            	
		            }
			}

		}

		private void handleGet(KVMessage request) {
			// TODO Auto-generated method stub
				if (kvServer.hasKey(request.getKey())){
				try {
					KVMessage response = new KVMessage(RESP);
					response.setKey(request.getKey());
					response.setValue(kvServer.get(request.getKey()));
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
			} 
				else {
					KVMessage response = new KVMessage(RESP,"Error");
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

				}
			}

		private void handleDel(KVMessage request) {
			// TODO Auto-generated method stub
			if(Handshakestate == 0){
				try {
					KVMessage response = new KVMessage("ready");
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
				tpcLog.appendAndFlush(request);
				Handshakestate ++;
			}
			
			else if (Handshakestate == 2){
		        try {
		            kvServer.del(request.getKey());
		            KVMessage response = new KVMessage("ack");
		            response.sendMessage(master);
		            Handshakestate = 0;
		        } catch (KVException e) {
	                KVMessage ErrorResponse = e.getKVMessage();
	                try {
						ErrorResponse.sendMessage(master);
					} catch (KVException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		            	
		            }
			}

			
		}

    }

}
