package kvstore;

import static kvstore.KVConstants.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Enumeration;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.io.StringWriter;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This is a basic key-value store. Ideally this would go to disk, or some other
 * backing store.
 */
public class KVStore implements KeyValueInterface {

    private ConcurrentHashMap<String, String> store;

    /**
     * Construct a new KVStore.
     */
    public KVStore() {
        resetStore();
    }

    private void resetStore() {
        this.store = new ConcurrentHashMap<String, String>();
    }

    /**
     * Insert key, value pair into the store.
     *
     * @param  key String key
     * @param  value String value
     */
    @Override
    public void put(String key, String value) {
        store.put(key, value);
    }

    /**
     * Retrieve the value corresponding to the provided key
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public String get(String key) throws KVException {
        String retVal = this.store.get(key);
        if (retVal == null) {
            KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
            throw new KVException(msg);
        }
        return retVal;
    }

    /**
     * Delete the value corresponding to the provided key.
     *
     * @param  key String key
     * @throws KVException with ERROR_NO_SUCH_KEY if key does not exist in store
     */
    @Override
    public void del(String key) throws KVException {
        if(key != null) {
            if (!this.store.containsKey(key)) {
                KVMessage msg = new KVMessage(KVConstants.RESP, ERROR_NO_SUCH_KEY);
                throw new KVException(msg);
            }
            this.store.remove(key);
        }
    }

    /**
     * Serialize this store to XML. See the spec for specific output format.
     * This method is best effort. Any exceptions that arise can be dropped.
     */
    public String toXML() {
    	try {
    		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    		DocumentBuilder db = dbf.newDocumentBuilder();
    		Document xmlDoc = db.newDocument();
    		xmlDoc.setXmlStandalone(true);
    		
    		Element root = xmlDoc.createElement("KVStore");
    		for (Enumeration<String> keys = store.keys(); keys.hasMoreElements();) {
    			String k = keys.nextElement();
    			Element pair = xmlDoc.createElement("KVPair");

    			Element key = xmlDoc.createElement("Key");
				key.appendChild(xmlDoc.createTextNode(k));
				Element value = xmlDoc.createElement("Value");
				value.appendChild(xmlDoc.createTextNode(store.get(k)));
				
				pair.appendChild(key);
				pair.appendChild(value);
    			
    			root.appendChild(pair);
    		}
    		xmlDoc.appendChild(root);
    		
    		Transformer transformer = null;
    	    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    		try {
    			transformer = transformerFactory.newTransformer();
    		} catch (TransformerConfigurationException e) {
    		}
    		
    		StringWriter xmlwriter = new StringWriter();
    		DOMSource source = new DOMSource(xmlDoc);
    		StreamResult xmlout = new StreamResult(xmlwriter);
    		
    		try {
    			transformer.transform(source, xmlout);
    		} catch (TransformerException e) {
    		}
    		
    		return xmlwriter.toString();
    	} catch (ParserConfigurationException e) {
    		return null;
    	}
    }

    @Override
    public String toString() {
        return this.toXML();
    }

    /**
     * Serialize to XML and write to a file.
     * This method is best effort. All exceptions that      *
     * @param fileName the file to write the serialized store
     */
    public void dumpToFile(String fileName) {
    	File dump = new File(fileName);
    	try {
    		
    		if (!dump.exists()) {
    			dump.createNewFile();
    		}
    		
    		BufferedWriter bw = new BufferedWriter(new FileWriter(dump.getAbsoluteFile()));
    		bw.write(this.toString());
    		bw.close();
    		
    	} catch (IOException e) {
    		System.out.println("Dump failed.");
    	}
    }

    /**
     * Replaces the contents of the store with the contents of a file
     * written by dumpToFile; the previous contents of the store are lost.
     * The store is cleared even if the file doesn't exist.
     * This method is best effort. Any exceptions that arise can be dropped.
     *
     * @param fileName the file containing the serialized store data
     */
    public void restoreFromFile(String fileName) {
        resetStore();
        File restore = new File(fileName);
    	try {
    
    		Scanner sr = new Scanner(restore);
    		String xml = "";
    		sr.next();
    	    while (sr.hasNext()) {
    	    	xml += sr.next();
    	    }
    	    
    	    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    	    DocumentBuilder db = null;
    	    try {
    	        db = dbf.newDocumentBuilder();
    	        InputSource is = new InputSource();
    	        is.setCharacterStream(new StringReader(xml));
    	        try {
    	            Document doc = db.parse(is);
    	            Element KVStore = doc.getDocumentElement();
    	            NodeList kvpairs = KVStore.getChildNodes();
    	            for (int i = 0; i < kvpairs.getLength(); i++) {
    	            	Node KVPair = kvpairs.item(i);
    	            	String key = KVPair.getFirstChild().getTextContent();
    	            	String value = KVPair.getLastChild().getTextContent();
    	            	store.put(key,  value);
    	            }
    	            sr.close();
    	        } catch (SAXException e) {
    	        	sr.close();
    	            throw new ParserConfigurationException();
    	        }
    	    } catch (ParserConfigurationException e) {
    	        
    	    }
    	} catch (IOException e) {
    		System.out.println("Restore failed.");
    	}
    	
    }
}
