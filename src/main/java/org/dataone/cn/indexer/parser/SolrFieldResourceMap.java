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

package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.dataone.cn.indexer.XMLNamespaceConfig;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 8/22/11
 * Time: 10:49 AM
 */

/**
 * Extracts resource map information and creates or updates relations in
 * documents in solr index.
 * 
 */
public class SolrFieldResourceMap extends SolrField {
    private DocumentBuilderFactory documentBuilderFactory = null;
    private static DocumentBuilder builder = null;

    private XPathFactory xpathFactory = null;
    private XPath xpathEval = null;
    private XMLNamespaceConfig xmlNamespaceConfig = null;
    private XPathExpression resourceExpression = null;

    String resourceMapRegexMatch = null;
    private String objectFormatXPath = null;
    private String baseUrl = null;
    private String resourceValueMatch = null;

    public SolrFieldResourceMap(String name, String xpath, String resourceMapXpath,
            boolean multivalue, XMLNamespaceConfig xmlNamespaceConfig)
            throws ParserConfigurationException {
        super(name, xpath, multivalue);

        xpathFactory = XPathFactory.newInstance();
        xpathEval = xpathFactory.newXPath();
        xpathEval.setNamespaceContext(xmlNamespaceConfig);
    }

    @Override
    public void initExpression(XPath xpathObject) {
        super.initExpression(xpathObject);
    }

    private boolean hasResourceMap(String value) {
        return value.matches(getResourceValueMatch());
    }

    @Override
    public List<SolrElementField> getFields(Document doc, String identifier)
            throws XPathExpressionException, IOException, SAXException,
            ParserConfigurationException {
        String value = (String) getxPathExpression().evaluate(doc, XPathConstants.STRING);

        if (hasResourceMap(value)) {
            String uri = getBaseUrl() + value;
            HttpClient client = new DefaultHttpClient();
            HttpUriRequest request = new HttpGet(uri);

            HttpResponse response = client.execute(request);
            HttpEntity responseEntity = response.getEntity();
            InputStream responseInputStream = responseEntity.getContent();
            Document resourceDocument = getBuilder().parse(responseInputStream);

            return processField(resourceDocument, resourceExpression, getName(), null, true, false);
        }

        return new ArrayList<SolrElementField>();
    }

    public String getObjectFormatXPath() {
        return objectFormatXPath;
    }

    public void setObjectFormatXPath(String objectFormatXPath) {
        this.objectFormatXPath = objectFormatXPath;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    private DocumentBuilder getBuilder() throws ParserConfigurationException {
        if (builder == null) {
            documentBuilderFactory = DocumentBuilderFactory.newInstance();
            documentBuilderFactory.setNamespaceAware(true);
            builder = documentBuilderFactory.newDocumentBuilder();
        }
        return builder;
    }

    public String getResourceValueMatch() {
        return resourceValueMatch;
    }

    public void setResourceValueMatch(String resourceValueMatch) {
        this.resourceValueMatch = resourceValueMatch;
    }
}
