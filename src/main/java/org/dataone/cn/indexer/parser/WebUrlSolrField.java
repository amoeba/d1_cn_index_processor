/**
 * 
 */
package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.cn.impl.v1.NodeRegistryService;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.NodeType;
import org.dataone.service.types.v1.util.NodelistUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This class implements an edge case where we are populating a multivalued
 * field that contains the download URLs for the object described by the record.
 * 
 * @author vieglais
 * 
 */
public class WebUrlSolrField extends SolrField {

    // do not use nodeMap directly - use getNodeMap() method
    // to ensure nodeMap is filled correctly.
    Map<String, String> nodeMap = null;

    private static final String V1_OBJECT_ENDPOINT = "/v1/object/";

    String nodesXPath = "//replica[replicationStatus/text()='completed']/replicaMemberNode/text()";
    XPathExpression nodesExpression = null;
    boolean onlyReferenceCNs = true;
    private static Logger logger = Logger.getLogger(WebUrlSolrField.class.getName());

    private NodeRegistryService nodeRegistryService;

    public WebUrlSolrField(String name, String xpath, String nodesXPath) {
        super(name, xpath);
        this.nodesXPath = nodesXPath;
    }

    public void setOnlyReferenceCNs(boolean onlyReferenceCNs) {
        this.onlyReferenceCNs = onlyReferenceCNs;
    }

    public boolean isNodeCN(String nodeName) {

        NodeReference nr = new NodeReference();
        nr.setValue(nodeName);
        try {
            org.dataone.service.types.v1.Node node = nodeRegistryService.getNode(nr);
            if (node != null) {
                return NodeType.CN.equals(node.getType().xmlValue());
            } else {
                return false;
            }
        } catch (ServiceFailure e) {
            e.printStackTrace();
            return false;
        } catch (NotFound e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void initExpression(XPath xpathObject) {
        super.initExpression(xpathObject);
        try {
            nodesExpression = xpathObject.compile(nodesXPath);
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getNodeMap() throws NotImplemented, ServiceFailure {
        if (this.nodeMap == null) {
            this.nodeMap = NodelistUtil.mapNodeList(nodeRegistryService.listNodes());
            logger.info("**********Found node registry with: " + nodeMap.size() + ".");
            logger.info("**********Node Map: " + nodeMap.toString());
        }
        return this.nodeMap;
    }

    /**
     * TODO: Need to update this method to use a resolve mechanism to determine
     * which node will become the sole source for the data_url field. Currently
     * the first node found is used.
     **/
    @Override
    public List<SolrElementField> processField(Document doc, XPathExpression expression,
            String name, IConverter converter, boolean multiValued, boolean xmlEscape)
            throws XPathExpressionException, IOException, SAXException,
            ParserConfigurationException {

        List<SolrElementField> fields = new ArrayList<SolrElementField>();

        try {
            // retrieve the identifier from the document
            String id = (String) expression.evaluate(doc, XPathConstants.STRING);

            if (converter != null) {
                id = converter.convert(id);
            }

            // Now create URLs for each of the download
            // retrieve the nodes from the sysmeta
            NodeList values = (NodeList) nodesExpression.evaluate(doc, XPathConstants.NODESET);
            for (int i = 0; i < values.getLength(); i++) {
                Node n = values.item(i);
                String nodeValue = n.getNodeValue();
                if (getNodeMap().containsKey(nodeValue)) {
                    if (onlyReferenceCNs) {
                        if (isNodeCN(nodeValue)) {
                            String getUrl = getNodeMap().get(nodeValue) + V1_OBJECT_ENDPOINT + id;
                            if (converter != null) {
                                getUrl = converter.convert(getUrl);
                            }
                            if (xmlEscape) {
                                getUrl = StringEscapeUtils.escapeXml(getUrl);
                            }
                            fields.add(new SolrElementField(name, getUrl));
                            logger.info("GET URL = " + getUrl);
                            if (!this.multivalue) {
                                return fields;
                            }
                        }
                    } else {
                        String getUrl = getNodeMap().get(nodeValue) + V1_OBJECT_ENDPOINT + id;
                        if (converter != null) {
                            getUrl = converter.convert(getUrl);
                        }
                        if (xmlEscape) {
                            getUrl = StringEscapeUtils.escapeXml(getUrl);
                        }
                        fields.add(new SolrElementField(name, getUrl));
                        logger.info("GET URL = " + getUrl);
                        if (!this.multivalue) {
                            return fields;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fields;
    }

    public void setNodeRegistryService(NodeRegistryService nrs) {
        this.nodeRegistryService = nrs;
    }
}
