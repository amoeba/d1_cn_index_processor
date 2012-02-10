package org.dataone.cn.indexer.parser.utility;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class RootElement {

    private String name;
    private String xPath;
    private XPathExpression xPathExpression;
    private String delimiter = " ";
    private String template;
    private List<LeafElement> leafs = new ArrayList<LeafElement>();
    private List<RootElement> subRoots = new ArrayList<RootElement>();
    private TemplateStringProcessor templateProcessor = new TemplateStringProcessor();

    public RootElement() {
    }

    public List<String> getRootValues(Object docOrNode, boolean multipleValues)
            throws XPathExpressionException {
        NodeList nodeList = (NodeList) getxPathExpression().evaluate(docOrNode,
                XPathConstants.NODESET);
        List<String> resultValues = new ArrayList<String>();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Map<String, String> valueMap = new HashMap<String, String>();
            Node node = nodeList.item(i);
            for (LeafElement leaf : getLeafs()) {
                valueMap.put(leaf.getName(), leaf.getLeafValue(node));
            }
            for (RootElement subRoot : getSubRoots()) {
                List<String> subRootResults = subRoot.getRootValues(node, multipleValues);
                StringBuilder delimitedResult = new StringBuilder();
                for (String result : subRootResults) {
                    delimitedResult.append(result);
                    delimitedResult.append(subRoot.getDelimiter());
                }
                valueMap.put(subRoot.getName(),
                        StringUtils.removeEnd(delimitedResult.toString(), delimiter));
            }
            // process valueMap with template string to create value for
            // root element per node in node list
            String templateResult = templateProcessor.process(getTemplate(), valueMap);
            resultValues.add(templateResult);
            if (!multipleValues) {
                break;
            }
        }
        return resultValues;
    }

    public void initXPathExpressions(XPath xPathObject) {
        try {
            xPathExpression = xPathObject.compile(xPath);
            for (LeafElement leaf : leafs) {
                leaf.initXPathExpression(xPathObject);
            }
            for (RootElement subRoot : subRoots) {
                subRoot.initXPathExpressions(xPathObject);
            }
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
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

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public List<LeafElement> getLeafs() {
        return leafs;
    }

    public void setLeafs(List<LeafElement> leafs) {
        this.leafs = leafs;
    }

    public List<RootElement> getSubRoots() {
        return subRoots;
    }

    public void setSubRoots(List<RootElement> subRoots) {
        this.subRoots = subRoots;
    }
}
