package org.dataone.cn.indexer.parser.utility;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class LeafElement {

    private String name;
    private String xPath;
    private XPathExpression xPathExpression;
    private String delimiter = " ";

    public LeafElement() {
    }

    public void initXPathExpression(XPath xPathObject) {
        try {
            xPathExpression = xPathObject.compile(xPath);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    public String getLeafValue(Node node) throws XPathExpressionException {
        StringBuilder value = new StringBuilder();
        NodeList nodeList = (NodeList) getxPathExpression().evaluate(node, XPathConstants.NODESET);
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node textNode = nodeList.item(i);
            if (textNode.getNodeValue() != null) {
                value.append(textNode.getNodeValue().trim());
                value.append(getDelimiter());
            }
        }
        return StringUtils.removeEnd(value.toString().trim(), delimiter);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getxPath() {
        return xPath;
    }

    public void setxPath(String xPath) {
        this.xPath = xPath;
    }

    public XPathExpression getxPathExpression() {
        return xPathExpression;
    }

    public void setxPathExpression(XPathExpression xPathExpression) {
        this.xPathExpression = xPathExpression;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }
}
