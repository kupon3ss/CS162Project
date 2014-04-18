package kvstore;

import static kvstore.KVConstants.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.net.*;
import java.util.*;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.*;
import org.xml.sax.SAXException;


public class KVMessageTest {

    private Socket sock;

    private static final String TEST_INPUT_DIR = "test/kvstore/test-inputs/";

    @Test
    public void successfullyParsesPutReq() throws KVException {
        setupSocket("putreq.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(PUT_REQ, kvm.getMsgType());
        assertNull(kvm.getMessage());
        assertNotNull(kvm.getKey());
        assertNotNull(kvm.getValue());
    }

    @Test
    public void successfullyParsesPutResp() throws KVException {
        setupSocket("putresp.txt");
        KVMessage kvm = new KVMessage(sock);
        assertNotNull(kvm);
        assertEquals(RESP, kvm.getMsgType());
        assertEquals("Success", kvm.getMessage());
        assertNull(kvm.getKey());
        assertNull(kvm.getValue());
    }
    
    @Test
    public void constructorTestbasic() throws KVException{
    	KVMessage kvm = new KVMessage("putreq");
    	KVMessage kvm2 = new KVMessage(kvm);
    	assertNotNull(kvm);
    	assertEquals("putreq", kvm.getMsgType());
    	assertNotNull(kvm2);
    	assertEquals("putreq", kvm2.getMsgType());
    	try {
			KVMessage kvm3 = new KVMessage("whateveryo");
		} catch (KVException e) {
			assertEquals(ERROR_INVALID_FORMAT, e.getKVMessage().getMessage());
		}
    }
    
    @Test
    public void basictoXMLtest() throws KVException{
        setupSocket("putreq.txt");
        KVMessage kvm = new KVMessage(sock);
        String xmlout = kvm.toXML();        
        System.out.println(xmlout);
        String sampleXML = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"putreq\"><Key>key</Key></KVMessage>");
        assertEquals(xmlout, sampleXML);
    }
 
    @Test
    public void basictoXMLtest2() throws KVException{
        setupSocket("resp.txt");
        KVMessage kvm = new KVMessage(sock);
        String xmlout = kvm.toXML();        
        System.out.println(xmlout);
        String sampleXML = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><KVMessage type=\"resp\"><Key>key</Key><Value>value</Value></KVMessage>");
        assertEquals(xmlout, sampleXML);
    }
 
    /*
    @Test
    public void basicSendRecieve() throws KVException{
        setupSocket("putreq.txt");
        KVMessage kvm = new KVMessage(sock);
        String xmlout = kvm.toXML();
        Socket sock2 = new Socket();
        kvm.sendMessage(sock);
        KVMessage kvm2 = new KVMessage(sock);
        System.out.print(kvm2.toXML());
    }
    */

    /* Begin helper methods */

    private void setupSocket(String filename) {
        sock = mock(Socket.class);
        File f = new File(System.getProperty("user.dir"), TEST_INPUT_DIR + filename);
        try {
            doNothing().when(sock).setSoTimeout(anyInt());
            when(sock.getInputStream()).thenReturn(new FileInputStream(f));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
