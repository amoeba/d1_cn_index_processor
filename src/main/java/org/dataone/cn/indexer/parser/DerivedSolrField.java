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

import com.hp.hpl.jena.query.Dataset;
import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.parser.utility.LogicalOrPostProcessor;
import org.dataone.cn.indexer.parser.utility.ElementCombiner;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

/**
 * Derive a Solr field value based on other values in the document.
 * This class works in conjunction with ElementCombiner, which performs the
 * retrieval of component values and combining them based on a template.
 */
public class DerivedSolrField implements ISolrDatasetField {

    private ElementCombiner base;
    private LogicalOrPostProcessor orProcessor = new LogicalOrPostProcessor();
    protected boolean multivalue = false;
    protected String name = null;
    protected IConverter converter = null;

    public DerivedSolrField(String name) {
        this.name = name;
    }

    public ElementCombiner getBase() {
        return base;
    }

    public void setBase(ElementCombiner base) { this.base = base; }

    /** Get the field values, which could be multiple values
     * @param  dataset A triple store containing triples constructed from the source document.
     * @return the field values, as Solr document fields
     */
    public List<SolrElementField> getFields(Dataset dataset) {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        if (base != null) {
            List<String> resultValues = base.getElementValues(dataset, isMultivalue());
            for (String value : resultValues) {
                if (orProcessor != null) {
                    value = orProcessor.process(value);
                }
                if (getConverter() != null) {
                    value = getConverter().convert(value);
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
    public List<SolrElementField> getFields(byte[] data, String identifier) throws Exception {
        return null;
    }

    /** Can this field contain multiple values.
     */
    public boolean isMultivalue() {
        return multivalue;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Controls whether the search index field this instance of SolrField is generating is defined
     * as accepting multiple values (a collection of values).
     *
     * @param multivalue
    */
    public void setMultivalue(boolean multivalue) {
        this.multivalue = multivalue;
    }

    /**
     * Get the method that is used to convert this value to a form that will be stored
     * in Solr.
     * @return the converter method
     */
    public IConverter getConverter() {
        return converter;
    }

    public void setConverter(IConverter converter) {
        this.converter = converter;
    }

}
