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
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Extension of SolrField.  Merges multiple data values into a single value field seperated
 * by the delimiter character.  Assumes a non-mulivalued field.  Used when the xPath selector
 * rule may match multiple text nodes which need to be combined into a single delimited value.
 * 
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
    public List<SolrElementField> processField(Document doc) throws XPathExpressionException,
            IOException, SAXException, ParserConfigurationException {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();

        try {
            NodeList values = (NodeList) xPathExpression.evaluate(doc, XPathConstants.NODESET);
            Set<String> usedValues = new HashSet<String>();
            StringBuilder sb = new StringBuilder();
            int imax = values.getLength();
            for (int i = 0; i < imax; i++) {
                Node n = values.item(i);
                String nodeValue = n.getNodeValue();
                if (nodeValue != null) {
                    nodeValue = nodeValue.trim();
                    if ((!dedupe) | (dedupe & !usedValues.contains(nodeValue))) {
                        if (allowedValue(nodeValue)) {
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
            }
            String nodeValue = sb.toString().trim();
            fields.add(new SolrElementField(name, nodeValue));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return fields;
    }
}
