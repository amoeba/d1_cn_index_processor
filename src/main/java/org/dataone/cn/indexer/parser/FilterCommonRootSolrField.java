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

import org.dataone.cn.indexer.parser.utility.FilterRootElement;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.parser.utility.LogicalOrPostProcessor;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

import javax.xml.xpath.XPath;
import java.util.ArrayList;
import java.util.List;



/**
 * A complex data value mining SolrField. This class returns a value to
 * an indexing subprocessor from its dependent class. See FilterRootElement for
 * a typical usage.
 * <p>
 *     Based on CommonRootSolrField by sroseboo
 * </p>
 *
 * @author slaughter
 *
 */
public class FilterCommonRootSolrField extends SolrField {

    private FilterRootElement root;

    private LogicalOrPostProcessor orProcessor = new LogicalOrPostProcessor();

    public FilterCommonRootSolrField(String name) {
        this.name = name;
    }

    /**
     * Prepare a Solr fields by extracting information from an input XML document, using the document processor configured
     * by Spring context files
     * @param doc the document to process
     * @param identifier a specific identifier to process
     * @return the Solr fields to be added to the index
     * @throws Exception
     * @see "application-context-collections.xml"
     */
    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {

        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        String resultValue = null;
        if (root != null) {
            resultValue = root.getRootValues(doc);
        }

        fields.add(new SolrElementField("collectionQuery", resultValue));
        return fields;
    }

    /**
     * Inialize the XPath expression that will be used to location XML nodes to process
     * @param xpathObject the XPath to initialize
     */
    @Override
    public void initExpression(XPath xpathObject) {
        root.initXPathExpressions(xpathObject);
    }

    /**
     * Get the root element that will be processed
     * @return the root element that will be processed
     */
    public FilterRootElement getRoot() {
        return root;
    }

    /**
     * Set the root element that will be processed
     * @param root the root element that will be processed
     */
    public void setRoot(FilterRootElement root) {
        this.root = root;
    }
}
