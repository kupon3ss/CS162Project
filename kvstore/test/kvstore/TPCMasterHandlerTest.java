package kvstore;

import static org.junit.Assert.*;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Random;
import org.junit.Test;

public class TPCMasterHandlerTest {


	    @Test
	    public void sanitysampletest() throws IOException, KVException {
	    	
	    	//Just checkss that we make a handler, most of the interaction tests are checked elsewhere 
	    	
		    String logPath;
		    TPCLog log;

		    KVServer keyServer;
		    SocketServer server;

		    long slaveID;
		    String masterHostname;

		    int masterPort = 8080;
		    int registrationPort = 9090;

	        Random rand = new Random();
	        slaveID = rand.nextLong();
	        masterHostname = InetAddress.getLocalHost().getHostAddress();
	        System.out.println("Looking for master at " + masterHostname);

	        keyServer = new KVServer(100, 10);
	        server = new SocketServer(InetAddress.getLocalHost().getHostAddress());

	        logPath = "bin/log." + slaveID + "@" + server.getHostname();
	        log = new TPCLog(logPath, keyServer);

	        TPCMasterHandler handler = new TPCMasterHandler(slaveID, keyServer, log);
	        server.addHandler(handler);
	        server.connect();

	        
	        assertTrue(handler != null);
	        assertTrue(log != null);
	    }

	}
