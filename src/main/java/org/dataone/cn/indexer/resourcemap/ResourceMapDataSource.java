package org.dataone.cn.indexer.resourcemap;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.cn.indexer.parser.IDocumentProvider;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/1/11
 * Time: 12:39 PM
 */


/**This class needs updated.  Services for obtaining resource maps was not available when created.
 *
 */


public class ResourceMapDataSource implements IDocumentProvider {

    private DocumentBuilder documentBuilder;
    private String baseURI = null;

    public ResourceMapDataSource() {

    }


    public Document GetDocument(String identifier) {
        HttpClient client = getClient();
        String uri = getResourceMapURI(identifier);
        HttpGet request = new HttpGet(uri);
        HttpResponse response = null;
        try {
            client.execute(request);
            Document doc = parseDocument(response.getEntity().getContent());
            return doc;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String getResourceMapURI(String identifier) {

        return getBaseURI() + identifier;
    }

    private Document parseDocument(InputStream inputStream) throws ParserConfigurationException, IOException, SAXException {
        return getDocumentBuilder().parse(inputStream);
    }



    public HttpClient getClient() {
        HttpClient client = null;
        if (client == null) {
            client = new DefaultHttpClient();
        }
        return client;
    }


    public DocumentBuilder getDocumentBuilder() throws ParserConfigurationException {
        if(documentBuilder == null){
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            documentBuilder = factory.newDocumentBuilder() ;
        }
        return documentBuilder;
    }

    public String getBaseURI() {
        return baseURI;
    }

    public void setBaseURI(String baseURI) {
        this.baseURI = baseURI;
    }
}
