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
