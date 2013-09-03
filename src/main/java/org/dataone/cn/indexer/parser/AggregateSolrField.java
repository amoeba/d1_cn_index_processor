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

import org.apache.commons.lang.StringUtils;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

/**
 * ISolrField implementation that aggregates multiple other SolrField instance configurations
 * into a new field value.
 * 
 * Used by some science metadata formats to create a full text field that contains different parts
 * of the science metadata document which cannot be derived with a single xPath selector.
 * 
 * For example - see the SolrField configuration in application-context-eml-base.xml at the full text
 * field.
 * 
 * @author sroseboo
 *
 */
public class AggregateSolrField implements ISolrField {

    private List<ISolrField> solrFields = null;
    private String name = null;

    @Override
    public void initExpression(XPath xpathObject) {
        for (ISolrField solrField : this.solrFields) {
            solrField.initExpression(xpathObject);
        }
    }

    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        StringBuffer finalValue = new StringBuffer();
        for (ISolrField solrField : this.solrFields) {
            List<SolrElementField> elements = solrField.getFields(doc, identifier);
            for (SolrElementField field : elements) {
                finalValue.append(field.getValue());
                finalValue.append(" ");
            }
        }
        fields.add(new SolrElementField(name, StringUtils.strip(finalValue.toString())));
        return fields;
    }

    public List<ISolrField> getSolrFields() {
        return solrFields;
    }

    public void setSolrFields(List<ISolrField> solrFieldList) {
        this.solrFields = solrFieldList;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
