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
import org.apache.commons.lang.StringUtils;
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
 * Base implementation of a class used to process an xml document in order
 * to mine value(s) from it - to be placed in a search index field.
 * 
 * SolrField defines:
 *      the search field name,
 *      the xpath expression that derives the field value.
 *      Control flags include whether the field is mapped to a multi-valued field,
 *      whether values should be de-duped (duplicates removed), whether a special
 *      conversion utility class should be run over the data values.
 *      
 */

public class SolrField implements ISolrField {
    protected String name = null;
    protected String xpath = null;
    protected boolean multivalue = false;
    protected XPathExpression xPathExpression = null;
    protected IConverter converter = null;
    // should be escaping values when serializing to XML, keep them are literal
    // values in the app
    protected boolean escapeXML = false;
    protected boolean combineNodes = false;
    protected boolean dedupe = false;
    protected List<String> disallowedValues = null;
    protected String valueSeparator = null;

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

    /**
     * Initializes the xPath expression parser object using xpath instance variable.
     */
    public void initExpression(XPath xpathObject) {
        if (getxPathExpression() == null) {
            try {
                setxPathExpression(xpathObject.compile(getXpath()));
            } catch (XPathExpressionException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns one or more elements of a single SOLR record.
     */
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        return processField(doc);
    }

    /**
     * Process incoming xml doc object for the data value this SolrField instance is
     * configured to derive.  Return a list of SolrElementFields containing the search
     * index field name and the data mined from the xml doc.
     * 
     * @param doc
     * @return
     * @throws XPathExpressionException
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public List<SolrElementField> processField(Document doc) throws XPathExpressionException,
            IOException, SAXException, ParserConfigurationException {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        Set<String> usedValues = new HashSet<String>();
        try {
            if (this.multivalue) {
                NodeList values = (NodeList) this.xPathExpression.evaluate(doc,
                        XPathConstants.NODESET);
                for (int i = 0; i < values.getLength(); i++) {
                    Node n = values.item(i);
                    String nodeValue = n.getNodeValue();
                    if (nodeValue != null) {
                        nodeValue = nodeValue.trim();

                        if (StringUtils.isNotEmpty(this.valueSeparator)
                                && nodeValue.contains(this.valueSeparator)) {
                            String[] inlineValues = StringUtils.split(nodeValue,
                                    this.valueSeparator);
                            for (int j = 0; j < inlineValues.length; j++) {
                                String inlineValue = inlineValues[j];
                                if (inlineValue.equals(this.valueSeparator) == false) {
                                    processValue(fields, usedValues, inlineValue);
                                }
                            }
                        } else {
                            processValue(fields, usedValues, nodeValue);
                        }
                    }
                }
            } else {
                String value = null;
                if (combineNodes) {
                    NodeList nodeSet = (NodeList) xPathExpression.evaluate(doc,
                            XPathConstants.NODESET);
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < nodeSet.getLength(); i++) {
                        if (i > 0) {
                            sb.append(" ");
                        }
                        Node nText = nodeSet.item(i);
                        String nodeValue = nText.getNodeValue();
                        if (nodeValue != null) {
                            nodeValue = nodeValue.trim();
                            if (this.converter != null) {
                                nodeValue = this.converter.convert(nodeValue);
                            }
                            if (this.escapeXML) {
                                nodeValue = StringEscapeUtils.escapeXml(nodeValue);
                            }
                            if (!dedupe || (dedupe && !usedValues.contains(nodeValue))) {
                                if (allowedValue(value)) {
                                    sb.append(nodeValue);
                                    usedValues.add(nodeValue);
                                }
                            }
                        }
                    }
                    value = sb.toString().trim();
                } else {
                    value = (String) xPathExpression.evaluate(doc, XPathConstants.STRING);
                    if (value != null) {
                        value = value.trim();
                    }
                    if (converter != null) {
                        value = converter.convert(value);
                    }
                    if (escapeXML) {
                        value = StringEscapeUtils.escapeXml(value);
                    }
                }
                if (StringUtils.isNotEmpty(value) && allowedValue(value)) {
                    fields.add(new SolrElementField(name, value));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fields;
    }

    private void processValue(List<SolrElementField> fields, Set<String> usedValues,
            String nodeValue) {
        if (StringUtils.isNotEmpty(nodeValue)) {
            nodeValue = nodeValue.trim();
            if (this.converter != null) {
                nodeValue = this.converter.convert(nodeValue);
            }
            if (this.escapeXML) {
                nodeValue = StringEscapeUtils.escapeXml(nodeValue);
            }
            if (!dedupe || (dedupe & !usedValues.contains(nodeValue))) {
                if (allowedValue(nodeValue)) {
                    fields.add(new SolrElementField(name, nodeValue));
                    usedValues.add(nodeValue);
                }
            }
        }
    }

    protected boolean allowedValue(String value) {
        if (this.disallowedValues == null || this.disallowedValues.isEmpty()) {
            return true;
        } else {
            for (String disallowed : this.disallowedValues) {
                if (disallowed.equalsIgnoreCase(value)) {
                    return false;
                }
            }
        }
        return true;
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

    /**
     * Controls whether duplicate values be removed from final field value.
     * 
     * @param dedupe
     */
    public void setDedupe(boolean dedupe) {
        this.dedupe = dedupe;
    }

    /**
     * Controls whether there are values which should be disallowed - removed from field value.
     * 
     * @param disallowed
     */
    public void setDisallowedValues(List<String> disallowed) {
        this.disallowedValues = disallowed;
    }

    public List<String> getDisallwedValues() {
        return this.disallowedValues;
    }

    /**
     * Delimiter character between values mined from source xml document.
     * 
     * @param valueSeparator
     */
    public void setValueSeparator(String valueSeparator) {
        this.valueSeparator = valueSeparator;
    }

    public String getValueSeparator() {
        return this.valueSeparator;
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

    /**
     * The name of the search index field this SolrField instance is generating.
     * 
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getXpath() {
        return xpath;
    }

    /**
     * A string representing an xPath selector rule used to derive search index field values
     * from incoming xml documents.
     * 
     * @param xpath
     */
    public void setXpath(String xpath) {
        this.xpath = xpath;
    }

    public boolean isMultivalue() {
        return multivalue;
    }

    /**
     * Controls whether the search index field this instance of SolrField is generating is defined
     * as accepting multiple values (a collection of values).
     * 
     * @param multivalue
     */
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
}
