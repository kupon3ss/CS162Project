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
    	try {
			sock.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
    	Socket sock = null;
    	try {
    		if(key == "" || key == null) throw new KVException(ERROR_INVALID_KEY);
    		if(value == "" || value == null) throw new KVException(ERROR_INVALID_VALUE);
    		
    		sock = connectHost();

    		KVMessage outMsg = new KVMessage(PUT_REQ);
    		outMsg.setKey(key);
    		outMsg.setValue(value);
    		outMsg.sendMessage(sock);
    		
    		KVMessage inMsg = new KVMessage(sock);
    		String message = inMsg.getMessage();
    		//assertTrue(message != null);
    		if(message == null) throw new KVException(ERROR_COULD_NOT_RECEIVE_DATA);
    		if( message != SUCCESS) throw new KVException(message);
    		
    	} catch (KVException kve) {
    		throw kve;
    	} finally {
    		if(sock != null) closeHost(sock);
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
    	Socket sock = null;
    	String toReturn = null;
        try {
        	if(key == "" || key == null) throw new KVException(ERROR_INVALID_KEY);
        	
        	sock = connectHost();
        	KVMessage outMsg = new KVMessage(GET_REQ);
        	outMsg.setKey(key);
        	outMsg.sendMessage(sock);
        	
        	KVMessage inMsg = new KVMessage(sock);
        	String message  = inMsg.getMessage();
        	
        	if(message != null) throw new KVException(message);
        	//Confirm that we got the right key back
        	if(inMsg.getKey() != key) throw new KVException(ERROR_COULD_NOT_RECEIVE_DATA);
        	toReturn = inMsg.getValue();

        } catch (KVException kve) {
        	throw kve;
        } finally {
        	if(sock != null) closeHost(sock);
        }
        return toReturn;
    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
    	Socket sock = null;
        try {
        	if(key == "" || key == null) throw new KVException(ERROR_INVALID_KEY);
        	
        	sock = connectHost();
        	
        	KVMessage outMsg = new KVMessage(DEL_REQ);
        	outMsg.setKey(key);
        	outMsg.sendMessage(sock);
        	
        	KVMessage inMsg = new KVMessage(sock);
        	String message = inMsg.getMessage();
        	if(message == null) throw new KVException(ERROR_COULD_NOT_RECEIVE_DATA);
        	if(message != SUCCESS) throw new KVException(message);

        } catch (KVException kve) {
        	throw kve;
        } finally {
        	if(sock != null) closeHost(sock);
        }
    }


}
