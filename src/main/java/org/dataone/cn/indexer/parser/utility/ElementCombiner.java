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

import com.hp.hpl.jena.query.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.indexer.annotation.SparqlField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

import java.util.*;

/**
 * A complex data value mining SolrField.
 * Multiple component elements from a document can be combined into a new, derived element.
 * This class is based on the CommonRootSolrField class (@sroseboo)
 *
 * The templateProcessor object defines how the component elements should be combined into
 * a new field value.
 * 
 * @author slaughter
 *
 */
public class ElementCombiner {

    private String name;
    private String delimiter = " ";
    private String template;
    private LinkedHashMap<String,Object> elements = new LinkedHashMap<>();
    private TemplateStringProcessor templateProcessor = new TemplateStringProcessor();
    private static Log log = LogFactory.getLog(ElementCombiner.class);

    public ElementCombiner() {}

    /**
     * Retrieve the component values from the document that will be used to construct a new
     * value. The values are formatted according to a template.
     * @param  dataset A triple store containing triples constructed from the source document.
     * @param multipleValues
     * @returna A list of element values
     */
    public List<String> getElementValues(Dataset dataset, boolean multipleValues) {
        List<String> resultValues = new ArrayList<>();
        Map<String, String> valueMap = new HashMap<String, String>();

        for (Map.Entry<String, Object> entry : elements.entrySet()) {
            String elementName = entry.getKey();
            log.trace("Found element: " + elementName);
            SparqlField element = (SparqlField) (entry).getValue();
            String q = null;
            //Get the Sparql query for this field
            q = element.getQuery();
            //Create a Query object
            Query query = QueryFactory.create(q);
            //Execute the Sparql query
            QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
            //Get the results of the query
            ResultSet results = qexec.execSelect();
            //Iterate over each query result and process it
            while (results.hasNext()) {
                //Create a SolrDoc for this query result
                QuerySolution solution = results.next();
                //Get the index field name and value returned from the Sparql query
                if (solution.contains(elementName)) {
                    //Get the value for this field
                    String value = solution.get(elementName).toString();
                    if (((SparqlField) element).getConverter() != null) {
                        value = ((SparqlField) element).getConverter().convert(value);
                    }
                    //Create an index field for this field name and value
                    SolrElementField f = new SolrElementField(elementName, value);
                    log.trace("JsonLdSubprocessor.process process the field " + elementName + "with value " + value);
                    valueMap.put(elementName, value);
                }
            }
            qexec.close();
        }

        // It's possible that the elements needed to fill in the template were not found in the
        // source document. If this is the case, return an empty list.
        if(!valueMap.isEmpty()) {
            String templateValue = getTemplate();
            String templateResult = templateProcessor.process(templateValue, valueMap);
            // This is another check to ensure that the values needed were available and
            // that the template has been filled out.
            if (templateValue.compareTo(templateResult) != 0) {
                resultValues.add(templateResult);
            }
        }

        return resultValues;
    }

    public String getName() { return name; }

    public void setName(String name) {
        this.name = name;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) { this.delimiter = delimiter; }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public LinkedHashMap<String,Object> getElements() {
        return elements;
    }

    public void setElements(LinkedHashMap<String, Object> elements) {
        this.elements = elements;
    }
}
