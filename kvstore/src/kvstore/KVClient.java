package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    private String server;
    private int port;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port to which the server is listening
     */
    public KVClient(String server, int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to make or connect socket
     */
    protected Socket connectHost() throws KVException {
        try {
        	return new Socket(this.server, this.port);
        } catch (UnknownHostException uhe) {
        	throw new KVException(ERROR_COULD_NOT_CONNECT);
        } catch (IOException ioe) {
        	throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        }
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param  sock Socket to be closed
     */
    protected void closeHost(Socket sock) {
        // implement me
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
    	try {
    		//Maybe connect in different try catch block in case we fail with an open socket
    		Socket sock = connectHost();
    		
    		KVMessage outMsg = new KVMessage(PUT_REQ);
    		outMsg.setKey(key);
    		outMsg.setValue(value);
    		outMsg.sendMessage(sock);
    		
    		KVMessage inMsg = new KVMessage(sock);
    		String message = inMsg.getMessage();
    		//assertTrue(message != null);
    		if( message != SUCCESS) throw new KVException(message);
    		
    		closeHost(sock);
    	} catch (KVException kve) {
    		// handle me
    	}
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
        try {
        	Socket sock = connectHost();
        	KVMessage outMsg = new KVMessage(GET_REQ);
        	outMsg.setKey(key);
        	outMsg.sendMessage(sock);
        	
        	KVMessage inMsg = new KVMessage(sock);
        	String message  = inMsg.getMessage();
        	if(message != SUCCESS) throw new KVException(message);
        	String toReturn = inMsg.getValue();
        	
        	closeHost(sock);
        	return toReturn;
        } catch (KVException kve) {
        	//handle me
        	return null;
        }
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
        try {
        	Socket sock = connectHost();
        	
        	KVMessage outMsg = new KVMessage(DEL_REQ);
        	outMsg.setKey(key);
        	outMsg.sendMessage(sock);
        	
        	KVMessage inMsg = new KVMessage(sock);
        	String message = inMsg.getMessage();
        	if(message != SUCCESS) throw new KVException(message);
        	
        	closeHost(sock);
        } catch (KVException kve) {
        	//handle me
        }
    }


}
