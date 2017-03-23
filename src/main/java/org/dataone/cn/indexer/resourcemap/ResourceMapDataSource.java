/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.indexer.resourcemap;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.cn.indexer.parser.IDocumentProvider;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

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


    public Document getDocument(String identifier) {
        HttpClient client = getClient();
        String uri = getResourceMapURI(identifier);
        HttpGet request = new HttpGet(uri);
        HttpResponse response = null;
        try {
            response = client.execute(request);
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
        Document doc = null;
        try {
            doc = getDocumentBuilder().parse(inputStream);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return doc;
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
