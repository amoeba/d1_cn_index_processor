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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.xml.xpath.XPathExpressionException;

import com.github.jsonldjava.core.DocumentLoader;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.annotation.SparqlField;
import org.dataone.cn.indexer.annotation.TripleStoreService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 * This sub-processor handles the index of the json-ld documents.
 * The documents will be loaded into a triple store, then SPARQL will be run.
 * @author tao
 *
 */
public class JsonLdSubprocessor implements IDocumentSubprocessor {
    private static Log log = LogFactory.getLog(JsonLdSubprocessor.class);
    private static PerformanceLogger perfLog = PerformanceLogger.getInstance();
    private List<String> matchDocuments = null;
    private List<ISolrDataField> fieldList = new ArrayList<ISolrDataField>();

    /**
     * Returns true if subprocessor should be run against object
     * 
     * @param formatId the the document to be processed
     * @return true if this processor can parse the formatId
     */
    public boolean canProcess(String formatId) {
        return matchDocuments.contains(formatId);
    }

    /**
     * Get the list of format ids which the subprocessor can process
     * @return the list of format ids which the subprocessor can process
     */
    public List<String> getMatchDocuments() {
        return matchDocuments;
    }

    /**
     * Set the list of format ids which the subprocessor can process
     * @param matchDocuments the format ids will be set
     */
    public void setMatchDocuments(List<String> matchDocuments) {
        this.matchDocuments = matchDocuments;
    }

    /**
     * Get the list of the Solr field names the subprocessor can handle
     * @return the list of Solr field names
     */
    public List<ISolrDataField> getFieldList() {
        return fieldList;
    }

    /**
     * Set the list of the Solr field names which the subprocessor can handle
     * @param fieldList  the list of Solr field names which the subprocessor can handle
     */
    public void setFieldList(List<ISolrDataField> fieldList) {
        this.fieldList = fieldList;
    }

    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            InputStream is) throws Exception {

        // ClassLoader oldContextCL = Thread.currentThread().getContextClassLoader();
        // try {
        //     Thread.currentThread().setContextClassLoader(JSONLD.class.getClassLoader());
        //     expandedJSON = JsonLdProcessor.expand(jsonObject);
        // } finally {
        //     // Restore, in case the current thread was doing something else
        //     // with the context classloader before calling our method
        //     Thread.currentThread().setContextClassLoader(oldContextCL);
        // }
        
        DocumentLoader dl;
        JsonLdOptions options = new JsonLdOptions();
        Map context = new HashMap();
        Object compactedJSONLD;
        Object object = JsonUtils.fromInputStream(is, "UTF-8");

        // Perform any necessary pre-processing on the original JSONLD document before
        // indexing
        Object expandedJSONLD = JsonLdProcessor.expand(object);
        
        if(isHttps((List) expandedJSONLD)) {
            log.debug("processing a JSONLD document containing an https://schema.org context");
            options = new JsonLdOptions();
            //options.setDocumentLoader(dl);
            context = new HashMap();
            context.put("@context", "https://schema.org/");
            compactedJSONLD = JsonLdProcessor.compact(expandedJSONLD, context, options);
            log.trace("JSON document after compaction: ");
            log.trace(JsonUtils.toPrettyString(compactedJSONLD));
        } else {
            log.debug("processing a JSONLD document containing an http://schema.org context");
            options = new JsonLdOptions();
            context = new HashMap();
            context.put("@context", "http://schema.org/");
            compactedJSONLD = JsonLdProcessor.compact(expandedJSONLD, context, options);
            log.trace("JSON document after compaction: ");
            log.trace(JsonUtils.toPrettyString(compactedJSONLD));
        }
        
        /**
         * Expand the document, forcing the @context to be 'http://schema.org'
         */

        String contextStr = (String) ((HashMap) compactedJSONLD).get("@context");
        log.trace("context in compacted doc: " + contextStr);

        // There appears to be a bug in the jsonld-java library that doesn't properly insert
        // the context into the document to be expanded. Therefor the context has to manually
        // inserted. The following commented lines are the documented method to use, that doesn't
        // work.
        //  context = new HashMap();
        //  context.put("@context", "http://schema.org/");
        //  options.setExpandContext(context);
        //  Object expanded = JsonLdProcessor.expand(compact, options);

        ((HashMap) compactedJSONLD).put("@context", "http://schema.org");
        expandedJSONLD = JsonLdProcessor.expand(compactedJSONLD);

        String str = JsonUtils.toString(expandedJSONLD);
        log.trace("JSON document after expand: " + str);
        is = new ByteArrayInputStream(str.getBytes());

        SolrDoc metaDocument = docs.get(identifier);
        if (metaDocument == null) {
            metaDocument = new SolrDoc();
            docs.put(identifier, metaDocument);
        }
        long start = System.currentTimeMillis();
        Map<String, SolrDoc> mergedDocuments;
        Dataset dataset = TripleStoreService.getInstance().getDataset();
        try {
            perfLog.log("JsonLdSubprocessor.process gets a dataset from tripe store service ", System.currentTimeMillis() - start);
            long startOntModel = System.currentTimeMillis();
            Model model = ModelFactory.createDefaultModel() ;
            model.read(is, "", "JSON-LD");
            dataset.getDefaultModel().add(model);
            perfLog.log("JsonLdSubprocessor.process adds the model ", System.currentTimeMillis() - startOntModel);
            //Track timing of this process
            long startField = System.currentTimeMillis();
            
            //Process each field listed in the fieldList in this subprocessor
            for (ISolrDataField field : this.fieldList) {
                long filed = System.currentTimeMillis();
                String q = null;
                
                //Process Sparql fields
                if (field instanceof SparqlField) {
                    //Get the Sparql query for this field
                    q = ((SparqlField) field).getQuery();
                    //Create a Query object
                    Query query = QueryFactory.create(q);
                    log.trace("Executing SPARQL query:\n" + query.toString());
                    //Execute the Sparql query
                    QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
                    //Get the results of the query
                    ResultSet results = qexec.execSelect();
                    //Iterate over each query result and process it
                    while (results.hasNext()) {
                        
                        //Create a SolrDoc for this query result
                        SolrDoc solrDoc = null;
                        QuerySolution solution = results.next();
                        log.trace(solution.toString());
                        //Get the index field name and value returned from the Sparql query
                        if (solution.contains(field.getName())) {
                            //Get the value for this field
                            String value = solution.get(field.getName()).toString();
                            if (((SparqlField) field).getConverter() != null) {
                                value = ((SparqlField) field).getConverter().convert(value);
                            }
                            //Create an index field for this field name and value
                            SolrElementField f = new SolrElementField(field.getName(), value);
                            log.debug("JsonLdSubprocessor.process process the field " + field.getName() + "with value " + value);
                            metaDocument.addField(f);
                        }
                    }
                }
                perfLog.log("JsonLdSubprocessor.process process the field " + field.getName(), System.currentTimeMillis() - filed);
            }
            perfLog.log("JsonLdSubprocessor.process() total take ", System.currentTimeMillis() - start);
        } finally {
            try {
                TripleStoreService.getInstance().destoryDataset(dataset);
            } catch (Exception e) {
                log.warn("A tdb directory can't be removed since "+e.getMessage(), e);
            }
        }
        return docs;
    }
    
    /**
     * Determine if the expanded jsonld object uses the schema of https://schema.org
     * @param expandedJsonld  the expanded Jsonld object
     * @return true if it uses https://schema.org; false if it uses http://schema.org
     */
    public boolean isHttps(List expandedJsonld) throws Exception {
        boolean https = false;
        for (int i=0; i< expandedJsonld.size(); i++) {
            Object obj = expandedJsonld.get(i);
            if(obj instanceof Map) {
                Map map = (Map) obj;
                Set keys = map.keySet();
                for (Object key : keys) {
                    log.debug("JsonLdSubProcess.isHttps - the key is " + key + " and value is " + map.get(key));
                    if (key instanceof String) {
                        if (((String)key).startsWith("https://schema.org")) {
                            https = true;
                            return https;
                        } else if (((String)key).startsWith("http://schema.org")) {
                            https = false;
                            return https;
                        }
                    } 
                }
            }
        }
        throw new Exception("The Processor cannot find the either prefix of https://schema.org or http://schema.org in the expanded json-ld object.");
    }
    
    @Override
    public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument) throws IOException,
            EncoderException, XPathExpressionException {
        // just return the given document
        return indexDocument;
    }
}
