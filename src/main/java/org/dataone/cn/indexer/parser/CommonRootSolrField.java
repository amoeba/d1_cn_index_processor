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

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPath;

import org.apache.commons.lang.StringEscapeUtils;
import org.dataone.cn.indexer.parser.utility.LogicalOrPostProcessor;
import org.dataone.cn.indexer.parser.utility.RootElement;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

/**
 * A complex data value mining SolrField.  For use when multiple xPath selector rules
 * need to be used together from a common rooted xml element.  However, using just xPath
 * selectors will ignore the 'common' root xml element requirement.  An example usage is 
 * in eml where creator elements are a common root which can contain a person name in
 * several elements OR an organization name.  The creator element needs to be treated as a
 * common root so that first and last name elements across creator elements are not erroneously
 * combined.
 * 
 * Uses a RootElement object to represent the common element from which the data values should
 * be derived.
 * 
 * @author sroseboo
 *
 */
public class CommonRootSolrField extends SolrField {

    private RootElement root;

    private LogicalOrPostProcessor orProcessor = new LogicalOrPostProcessor();

    public CommonRootSolrField(String name) {
        this.name = name;
    }

    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        if (root != null) {
            List<String> resultValues = root.getRootValues(doc, isMultivalue());
            for (String value : resultValues) {
                if (getConverter() != null) {
                    value = getConverter().convert(value);
                }
                if (isEscapeXML()) {
                    value = StringEscapeUtils.escapeXml(value);
                }

                if (orProcessor != null) {
                    value = orProcessor.process(value);
                }
                if (value != null && !value.isEmpty()) {
                    fields.add(new SolrElementField(this.name, value));
                }
                if (!isMultivalue()) {
                    break;
                }
            }
        }
        return fields;
    }

    @Override
    public void initExpression(XPath xpathObject) {
        root.initXPathExpressions(xpathObject);
    }

    public RootElement getRoot() {
        return root;
    }

    public void setRoot(RootElement root) {
        this.root = root;
    }
}
