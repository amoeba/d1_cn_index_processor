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

/**
 * Used by CommonRootSolrField.  Contains other root elements and leaf elements.
 * Allows definition of nested root elements which expand eventually into leaf elements.
 * 
 * Leaf elements represent text node elements which are endpoints of root elements.
 * 
 * The templateProcessor object defines how the leaf node data should be combined into
 * a new field value.
 * 
 * @author sroseboo
 *
 */
public class RootElement {

    private String name;
    private String xPath;
    private XPathExpression xPathExpression = null;
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
            if (xPathExpression == null) {
                xPathExpression = xPathObject.compile(xPath);
            }
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
