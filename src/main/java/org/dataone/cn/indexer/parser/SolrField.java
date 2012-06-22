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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang.StringEscapeUtils;
import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * User: Porter
 * Date: 7/25/11
 * Time: 1:40 PM
 */

/**
 * Used for mapping XPath queries to SolrIndex fields
 * 
 */

public class SolrField implements ISolrField {
    protected String name = null;
    protected String xpath = null;
    public boolean multivalue = false;
    protected XPathExpression xPathExpression = null;
    private IConverter converter = null;
    // should be escaping values when serializing to XML, keep them are literal
    // values in the app
    private boolean escapeXML = false;
    private boolean combineNodes = false;
    protected boolean dedupe = false;

    public SolrField() {
    }

    public SolrField(String name, String xpath, boolean multivalue) {
        this(name, xpath, multivalue, null);
    }

    public SolrField(String name, String xpath, boolean multivalue, IConverter converter) {
        this.name = name;
        this.xpath = xpath;
        this.multivalue = multivalue;
        this.converter = converter;
    }

    public SolrField(String name, String xpath) {
        this.name = name;
        this.xpath = xpath;
    }

    public void initExpression(XPath xpathObject) {

        try {
            setxPathExpression(xpathObject.compile(getXpath()));
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }

    }

    protected XPathExpression getxPathExpression() {
        return xPathExpression;
    }

    protected void setxPathExpression(XPathExpression xPathExpression) {
        this.xPathExpression = xPathExpression;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getXpath() {
        return xpath;
    }

    public void setXpath(String xpath) {
        this.xpath = xpath;
    }

    public boolean isMultivalue() {
        return multivalue;
    }

    public void setMultivalue(boolean multivalue) {
        this.multivalue = multivalue;
    }

    public IConverter getConverter() {
        return converter;
    }

    public void setConverter(IConverter converter) {
        this.converter = converter;
    }

    public boolean isEscapeXML() {
        return escapeXML;
    }

    public void setEscapeXML(boolean escapeXML) {
        this.escapeXML = escapeXML;
    }

    /**
     * Returns one or more elements of a single SOLR record.
     */
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        return processField(doc, getxPathExpression(), name, converter, multivalue, escapeXML);
    }

    public List<SolrElementField> processField(Document doc, XPathExpression expression,
            String name, IConverter converter, boolean multiValued, boolean xmlEscape)
            throws XPathExpressionException, IOException, SAXException,
            ParserConfigurationException {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        Set<String> usedValues = new HashSet<String>();
        try {
            if (multiValued) {
                NodeList values = (NodeList) expression.evaluate(doc, XPathConstants.NODESET);
                for (int i = 0; i < values.getLength(); i++) {
                    Node n = values.item(i);
                    String nodeValue = n.getNodeValue();
                    if (nodeValue != null) {
                        nodeValue = nodeValue.trim();
                        if (converter != null) {
                            nodeValue = converter.convert(nodeValue);
                        }
                        if (xmlEscape) {
                            nodeValue = StringEscapeUtils.escapeXml(nodeValue);
                        }
                        if (!dedupe || (dedupe & !usedValues.contains(nodeValue))) {
                            fields.add(new SolrElementField(name, nodeValue));
                            usedValues.add(nodeValue);
                        }
                    }
                }
            } else {

                String value = null;
                if (combineNodes) {
                    NodeList nodeSet = (NodeList) expression.evaluate(doc, XPathConstants.NODESET);
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < nodeSet.getLength(); i++) {
                        if (i > 0) {
                            sb.append(" ");
                        }
                        Node nText = nodeSet.item(i);
                        if (nText.getNodeValue() != null) {
                            if (!dedupe
                                    || (dedupe && !usedValues.contains(nText.getNodeValue().trim()))) {
                                sb.append(nText.getNodeValue().trim());
                                usedValues.add(nText.getNodeValue().trim());
                            }
                        }
                    }
                    value = sb.toString().trim();

                } else {
                    value = (String) expression.evaluate(doc, XPathConstants.STRING);
                    value = value.trim();
                }

                if (converter != null) {
                    value = converter.convert(value);
                }
                if (xmlEscape) {
                    value = StringEscapeUtils.escapeXml(value);
                }
                fields.add(new SolrElementField(name, value));

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return fields;
    }

    public boolean isCombineNodes() {
        return combineNodes;
    }

    /**
     * If set results are concatenated together using single space as separator.
     * this value is ignored if it is a multi value field
     * 
     * @param combineNodes
     */
    public void setCombineNodes(boolean combineNodes) {
        this.combineNodes = combineNodes;
    }

    public void setDedupe(boolean dedupe) {
        this.dedupe = dedupe;
    }
}
