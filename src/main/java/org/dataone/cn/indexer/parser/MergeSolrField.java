/**
 * 
 */
package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
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
 * @author vieglais
 * 
 */
public class MergeSolrField extends SolrField {

    private String delimiter = " ";

    public MergeSolrField(String name, String xpath) {
        super(name, xpath);
    }

    public MergeSolrField(String name, String xpath, String delimiter) {
        super(name, xpath);
        this.delimiter = delimiter;
    }

    @Override
    public List<SolrElementField> processField(Document doc, XPathExpression expression,
            String name, IConverter converter, boolean multiValued, boolean xmlEscape)
            throws XPathExpressionException, IOException, SAXException,
            ParserConfigurationException {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();

        try {
            NodeList values = (NodeList) expression.evaluate(doc, XPathConstants.NODESET);
            Set<String> usedValues = new HashSet<String>();
            StringBuilder sb = new StringBuilder();
            int imax = values.getLength();
            for (int i = 0; i < imax; i++) {
                Node n = values.item(i);
                String nodeValue = n.getNodeValue();
                if (nodeValue != null) {
                    nodeValue = nodeValue.trim();
                    if ((!dedupe) | (dedupe & !usedValues.contains(nodeValue))) {
                        sb.append(nodeValue);
                        if (i < imax - 1) {
                            sb.append(delimiter);
                        }
                        if (dedupe) {
                            usedValues.add(nodeValue);
                        }
                    }
                }
            }
            String nodeValue = sb.toString().trim();
            if (xmlEscape) {
                nodeValue = StringEscapeUtils.escapeXml(nodeValue);
            }
            fields.add(new SolrElementField(name, nodeValue));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fields;
    }
}
