package kvstore;

import static kvstore.KVConstants.*;

import java.io.*;
import java.net.*;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

/**
 * This is the object that is used to generate the XML based messages
 * for communication between clients and servers.
 */
public class KVMessage implements Serializable {

    private String msgType;
    private String key;
    private String value;
    private String message;
    
    private static final String[] SET_TYPES = new String[] { GET_REQ, PUT_REQ, DEL_REQ, RESP,REGISTER, COMMIT, ABORT, READY, ACK};
    private static final Set<String> msgTypes = new HashSet<String>(Arrays.asList(SET_TYPES));
    
    private static final String[] ELEMENT_TYPES = new String[] { "Value", "Key", "Message", "#text" };
    //private static final String VAL = "Value", KEY = "Key", MESS = "Message", TEXT = "#text";
    private static final Set<String> eleTypes = new HashSet<String>(Arrays.asList(ELEMENT_TYPES));
    
    public static final long serialVersionUID = 6473128480951955693L;
    
    public KVMessage(KVMessage kvm) {
        // implement me
    	this.msgType = kvm.getMsgType();
    	this.key = kvm.getKey();
    	this.value = kvm.getValue();
    	this.message = kvm.getMessage();
    }

    /**
     * Construct KVMessage with only a type.
     *
     * @param msgType the type of this KVMessage
     * @throws KVException 
     */
    public KVMessage(String msgType) throws KVException {
        if (!msgTypes.contains(msgType)){
        	throw new KVException(ERROR_INVALID_FORMAT);
        }
        else {
        	this.msgType = msgType;
        }
    }

    /**
     * Construct KVMessage with type and message.
     *
     * @param msgType the type of this KVMessage
     * @param message the content of this KVMessage
     */
    public KVMessage(String msgType, String message) {
        this.msgType = msgType;
        this.message = message;
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * Parse XML from the InputStream with unlimited timeout.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock) throws KVException{
        this(sock, 0);
    }

    /**
     * Construct KVMessage from the InputStream of a socket.
     * This constructor parse XML from the InputStream within a certain timeout
     * or with no timeout if the provided argument is 0.
     *
     * @param  sock Socket to receive serialized KVMessage through
     * @param  timeout total allowable receipt time, in milliseconds
     * @throws KVException if we fail to create a valid KVMessage. Please see
     *         KVConstants.java for possible KVException messages.
     */
    public KVMessage(Socket sock, int timeout) throws KVException {
        // implement me
    	Document document;
    	try {
	    	sock.setSoTimeout(timeout);
	    	NoCloseInputStream openStream = new NoCloseInputStream( sock.getInputStream() );
	        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	        DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			document = builder.parse(openStream);

    	} catch (SAXException e) {
			throw new KVException(ERROR_INVALID_FORMAT);
		} catch (IOException e) {
			throw new KVException(ERROR_COULD_NOT_RECEIVE_DATA);
		} catch (ParserConfigurationException e) {
			throw new KVException(ERROR_PARSER);
		} catch (NullPointerException e){
			throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
		}
    	
		Element root = document.getDocumentElement();
		NodeList elements = root.getChildNodes();

		
		if (!msgTypes.contains(root.getAttribute("type"))){
			throw new KVException(ERROR_INVALID_FORMAT);
    	}
        else if (root.getAttribute("type").equals(PUT_REQ)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {

    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
    			
                if (elementtype.equals("Key")) {
                	this.key = element.getTextContent();
                }
                else if (elementtype.equals("Value")) {
                	this.value = element.getTextContent();
                }
                else if (elementtype.equals("Message")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key == null || this.key.equals("") || this.value == null || this.value.equals("")){
    			throw new KVException(ERROR_INVALID_FORMAT);
    	    }
    	}
    	else if (root.getAttribute("type").equals(GET_REQ)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
    			
                if (elementtype.equals("Key")) {
                	this.key = element.getTextContent();
                }
                else if (elementtype.equals("Value")||elementtype.equals("Message")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key == null || this.key.equals("")){
				throw new KVException(ERROR_INVALID_FORMAT);
			}
    	}
    	else if (root.getAttribute("type").equals(DEL_REQ)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
    			
                if (elementtype.equals("Key")) {
                	this.key = element.getTextContent();
                }
                else if (elementtype.equals("Value")||elementtype.equals("Message")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key == null || this.key.equals("")){
				throw new KVException(ERROR_INVALID_FORMAT);
			}
    	}
    	// need a check and bulletproofing here
   
    	else if (root.getAttribute("type").equals(RESP)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
    			
                if (elementtype.equals("Key")) {
                	this.key = element.getTextContent();
                } 
                else if (elementtype.equals("Value")) {
                	this.value = element.getTextContent();
                }
                else if (elementtype.equals("Message")) {
                	this.message = element.getTextContent();
                }
                //System.out.println(elementtype);
    		}
    		if (this.message != null){
    			if (this.key != null || this.value != null){
    				throw new KVException(ERROR_INVALID_FORMAT);
    			}
    		}
    		else if (this.key == null || this.value == null){ 
				throw new KVException(ERROR_INVALID_FORMAT);

    		}
    	}
		
    	else if (root.getAttribute("type").equals(REGISTER)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
    			
                if (elementtype.equals("Message")) {
                	this.message = element.getTextContent();
                }
                else if (elementtype.equals("Value")||elementtype.equals("Key")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key != null || this.value != null){
				throw new KVException(ERROR_INVALID_FORMAT);
			}
    	}
		
    	else if (root.getAttribute("type").equals(REGISTER)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
   
    			if (elementtype.equals("Value")||elementtype.equals("Key")||elementtype.equals("Message")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key != null || this.value != null || this.message != null){
				throw new KVException(ERROR_INVALID_FORMAT);
			}
    	}
		
    	else if (root.getAttribute("type").equals(COMMIT) || root.getAttribute("type").equals(READY) || root.getAttribute("type").equals(ACK)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
   
    			if (elementtype.equals("Value")||elementtype.equals("Key")||elementtype.equals("Message")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key != null || this.value != null || this.message != null){
				throw new KVException(ERROR_INVALID_FORMAT);
			}
    	}
		
    	else if (root.getAttribute("type").equals(ABORT)){
    		this.msgType = root.getAttribute("type");
    		for (int i = 0; i < elements.getLength(); ++i) {
    			Node element = elements.item(i);
    			String elementtype = element.getNodeName();
    			
    			//check that its an acceptable field
    			if (!eleTypes.contains(elementtype)){
    				throw new KVException(ERROR_INVALID_FORMAT);
    	    	}
    			
                if (elementtype.equals("Message")) {
                	this.message = element.getTextContent();
                }
                else if (elementtype.equals("Value")||elementtype.equals("Key")){
        			throw new KVException(ERROR_INVALID_FORMAT);
                }
    		}
    		if (this.key != null || this.value != null){
				throw new KVException(ERROR_INVALID_FORMAT);
			}
    	}
		
		
    	
    	
		
    	
    }
    
 /*  Probably dont need this, can be used in KVServer  
  * 
    public void formatCheck() throws KVException{
    	if (msgTypes.contains(this.msgType)){
    		throw new KVException (ERROR_INVALID_FORMAT);
    	}
    	if (this.msgType.equals(PUT_REQ)){
    		if (this.key != null){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if (this.key.isEmpty()){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if ((this.key.length() > 256)){
    			throw new KVException (ERROR_OVERSIZED_KEY);
    		}
    		if (this.value != null){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if (this.value.isEmpty()){
    			throw new KVException (ERROR_INVALID_VALUE);
    		}
    		if (this.value.length() > 1024*256){
    			throw new KVException (ERROR_OVERSIZED_VALUE);
    		}
    	}
    	if (this.msgType.equals(GET_REQ)){
    		if (this.key != null){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if (this.key.isEmpty()){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if ((this.key.length() > 256)){
    			throw new KVException (ERROR_OVERSIZED_KEY);
    		}
    	}
    	if (this.msgType.equals(DEL_REQ)){
    		if (this.key != null){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if (this.key.isEmpty()){
    			throw new KVException (ERROR_INVALID_KEY);
    		}
    		if ((this.key.length() > 256)){
    			throw new KVException (ERROR_OVERSIZED_KEY);
    		}
    	}
    	if (this.msgType.equals(RESP)){
    		
    	}
    }
    
    */

    
    /*
     *    	
     *  if (!msgTypes.contains(this.msgType)){
    		return false;
    	}
    	if (this.msgType.equals(GET_REQ)){
    		
    	}
    	if (this.msgType.equals(PUT_REQ)){
    		
    	}
    	if (this.msgType.equals(DEL_REQ)){
    		
    	}
    	if (this.msgType.equals(RESP)){
    		
    	} 
     */
    /**
     * Generate the serialized XML representation for this message. See
     * the spec for details on the expected output format.
     *
     * @return the XML string representation of this KVMessage
     * @throws KVException with ERROR_INVALID_FORMAT or ERROR_PARSER
     */
    public String toXML() throws KVException{
        // implement me
    	Document xmldoc;
    	DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    	DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
		} catch (ParserConfigurationException e1) {
			throw new KVException ("XML Builder Error");
		}
    	xmldoc = builder.newDocument();
    	
    	Element xmlroot = xmldoc.createElement("KVMessage");
		xmlroot.setAttribute("type", this.msgType);
		xmldoc.appendChild(xmlroot);
		xmldoc.setXmlStandalone(true);
		
		//check for sanity?
    	
	    /*if (!msgTypes.contains(this.msgType)){

	    }*/
	    if (this.msgType.equals(PUT_REQ)){
	    	Element xmlkey = xmldoc.createElement("Key");
			xmlkey.appendChild(xmldoc.createTextNode(this.key));
			Element xmlval = xmldoc.createElement("Value");
			xmlval.appendChild(xmldoc.createTextNode(this.value));	
			xmlroot.appendChild(xmlkey);
			xmlroot.appendChild(xmlval);
	    }
	    else if (this.msgType.equals(GET_REQ)){
	    	Element xmlkey = xmldoc.createElement("Key");
			xmlkey.appendChild(xmldoc.createTextNode(this.key));
			xmlroot.appendChild(xmlkey);
	    }
	    else if (this.msgType.equals(DEL_REQ)){
	    	Element xmlkey = xmldoc.createElement("Key");
			xmlkey.appendChild(xmldoc.createTextNode(this.key));
			xmlroot.appendChild(xmlkey);
	    }
	    //not sure how to bulletproof the responses
	    else if (this.msgType.equals(RESP)){
    		if (this.key != null && !this.key.isEmpty()) {
		    	Element xmlkey = xmldoc.createElement("Key");
				xmlkey.appendChild(xmldoc.createTextNode(this.key));
				xmlroot.appendChild(xmlkey);
	    	}
	    	if (this.value != null && !this.value.isEmpty()) {
				Element xmlval = xmldoc.createElement("Value");
				xmlval.appendChild(xmldoc.createTextNode(this.value));	
				xmlroot.appendChild(xmlval);
	    	}
	    	if (this.message != null && !this.message.isEmpty()) {
				Element xmlmsg = xmldoc.createElement("Message");
				xmlmsg.appendChild(xmldoc.createTextNode(this.message));	
				xmlroot.appendChild(xmlmsg);
	    	}
	    }
	    
	    else if (this.msgType.equals(REGISTER) || this.msgType.equals(ABORT)){
	    	if (this.message != null && !this.message.isEmpty()) {
				Element xmlmsg = xmldoc.createElement("Message");
				xmlmsg.appendChild(xmldoc.createTextNode(this.message));	
				xmlroot.appendChild(xmlmsg);
	    	}
	    }
	    
	    else if (this.msgType.equals(READY) || this.msgType.equals(ABORT) || this.msgType.equals(COMMIT) || this.msgType.equals(ACK)){
	    //dont need to do anything here
	    }
	    
	    
	    Transformer transformer = null;
	    TransformerFactory transformerFactory = TransformerFactory.newInstance();
		try {
			transformer = transformerFactory.newTransformer();
		} catch (TransformerConfigurationException e) {
			
		}
		
		StringWriter xmlwriter = new StringWriter();

		DOMSource source= new DOMSource(xmldoc);
		StreamResult xmlout = new StreamResult(xmlwriter);
 
		try {
			transformer.transform(source, xmlout);
		} catch (TransformerException|NullPointerException e) {
			throw new KVException (ERROR_INVALID_FORMAT);
		}
        return xmlwriter.toString();
    }


    /**
     * Send serialized version of this KVMessage over the network.
     * You must call sock.shutdownOutput() in order to flush the OutputStream
     * and send an EOF (so that the receiving end knows you are done sending).
     * Do not call close on the socket. Closing a socket closes the InputStream
     * as well as the OutputStream, stopping the receipt of a response.
     *
     * @param  sock Socket to send XML through
     * @throws KVException with ERROR_INVALID_FORMAT, ERROR_PARSER, or
     *         ERROR_COULD_NOT_SEND_DATA
     */
    public void sendMessage(Socket sock) throws KVException {
        // implement me
    	try {
			String xmlmessage = this.toXML();
			PrintWriter socketout = new PrintWriter(sock.getOutputStream(), true);
			socketout.print(xmlmessage);
			socketout.flush();
			sock.shutdownOutput();
		} catch (IOException e) {
			throw new KVException (ERROR_COULD_NOT_SEND_DATA);
		}
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMsgType() {
        return msgType;
    }


    @Override
    public String toString() {
        try {
            return this.toXML();
        } catch (KVException e) {
            // swallow KVException
            return e.toString();
        }
    }

    /*
     * InputStream wrapper that allows us to reuse the corresponding
     * OutputStream of the socket to send a response.
     * Please read about the problem and solution here:
     * http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html
     */
    private class NoCloseInputStream extends FilterInputStream {
        public NoCloseInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() {} // ignore close
    }


}
