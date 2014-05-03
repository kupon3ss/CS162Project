package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

    private long slaveID;
    private String hostname;
    private int port;

    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String info) throws KVException {
        // implement me
    	Pattern getID = Pattern.compile("(.+)@");
    	Pattern getPort = Pattern.compile(":(.+)");
    	Pattern getHostname = Pattern.compile("@(.+):");

    	Matcher match = getID.matcher(info);
    	if (match.find()) {
    		try {
    			this.slaveID = new Long(match.group());
    		} catch (NumberFormatException nfe) {
    			throw new KVException(ERROR_INVALID_FORMAT);
    		}
    	} else {
    		throw new KVException(ERROR_INVALID_FORMAT);
    	}
    	
    	match = getPort.matcher(info);
    	if (match.find()) {
    		try {
    			this.port = new Integer(match.group());
    		} catch (NumberFormatException nfe) {
    			throw new KVException(ERROR_INVALID_FORMAT);
    		}
    	} else {
    		throw new KVException(ERROR_INVALID_FORMAT);
    	}
    	
    	match = getHostname.matcher(info);
    	if (match.find()) {
    		this.hostname = match.group();
    	} else {
    		throw new KVException(ERROR_INVALID_FORMAT);
    	}
    	
    	//This works too and covers all cases. Not as clean though. 
    	/* 
    	String[] parts0 = info.split("@");
    	String[] parts1 = parts0[1].split(":");
    	
    	if (parts0.length != 2 && parts1.length != 2) {
    		throw new KVException(ERROR_INVALID_FORMAT);
    	}
    	
    	try {
    		this.slaveID = new Long(parts0[0]);
    	} catch (NumberFormatException nfe) {
    			throw new KVException(ERROR_INVALID_FORMAT);
    	}
    	
    	try {
    		this.port = new Long(parts1[1]);
    	} catch (NumberFormatException nfe) {
    			throw new KVException(ERROR_INVALID_FORMAT);
    	}
    	
    	this.hostname = parts1[1];
    	 */
    	
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        // implement me
    	Socket sock = new Socket();
    	ServerSocket server;
    	try {
	    	server = new ServerSocket(getPort());
    	} catch (IOException ioe) {
    		throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
    	}
    	try {
    		sock.connect(server.getLocalSocketAddress(), timeout);
    	} catch (SocketTimeoutException ste) {
    		throw new KVException(ERROR_SOCKET_TIMEOUT);
    	} catch (IOException ioe) {
    		throw new KVException(ERROR_COULD_NOT_CONNECT);
    	} 
        return sock;
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) {
    	try {
    		sock.close();
    	} catch (IOException e) {
    		//Error ignored
    	}
    }
}
