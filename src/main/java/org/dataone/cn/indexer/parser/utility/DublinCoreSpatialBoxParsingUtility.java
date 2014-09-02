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

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;

public class DublinCoreSpatialBoxParsingUtility {

    private static Logger logger = Logger.getLogger(DublinCoreSpatialBoxParsingUtility.class
            .getName());

    private static final String DC_BOX_TOKEN_DELIMITER = ";";
    private static final String DC_BOX_TOKEN_VALUE_DELIMITER = "=";

    public static final String DC_BOX_NORTH_PROPERTY = "northlimit";
    public static final String DC_BOX_SOUTH_PROPERTY = "southlimit";
    public static final String DC_BOX_EAST_PROPERTY = "eastlimit";
    public static final String DC_BOX_WEST_PROPERTY = "westlimit";

    public String extractNodeValue(Document doc, XPathExpression xPathExpression) {
        String nodeValue = null;
        try {
            nodeValue = (String) xPathExpression.evaluate(doc, XPathConstants.STRING);
            //            NodeList nodeSet = (NodeList) xPathExpression.evaluate(doc, XPathConstants.NODESET);
            //            Node nText = nodeSet.item(1);
            //            if (nText != null) {
            //                nodeValue = nText.getNodeValue();
            //            }
        } catch (XPathExpressionException e) {
            logger.error("Error in XPath expression parsing for DC Spatial Box", e);
        }
        return nodeValue;
    }

    public String extractDirectionalValue(String nodeValue, String boxDirectionProperty) {
        String finalValue = null;

        if (nodeValue != null && StringUtils.isNotEmpty(nodeValue)) {
            nodeValue = nodeValue.trim();
            String[] tokens = StringUtils.split(nodeValue, DC_BOX_TOKEN_DELIMITER);
            for (int tokenCount = 0; tokenCount < tokens.length; tokenCount++) {
                String token = tokens[tokenCount];
                token = token.trim();
                if (token != null && StringUtils.isNotEmpty(token)
                        && StringUtils.contains(token, boxDirectionProperty)) {

                    String directionValue = StringUtils.substringAfter(token,
                            DC_BOX_TOKEN_VALUE_DELIMITER);
                    if (StringUtils.isNotEmpty(directionValue)) {
                        finalValue = directionValue.trim();
                    }

                    break;
                }
            }
        }
        return finalValue;
    }
}
