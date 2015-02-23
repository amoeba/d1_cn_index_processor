/**
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.dataone.cn.indexer.annotation;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.indexer.parser.IDocumentSubprocessor;
import org.dataone.cn.indexer.parser.ISolrDataField;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.springframework.beans.factory.annotation.Autowired;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.tdb.TDBFactory;


/**
 * A solr index parser for an RDF/XML file.
 * The solr doc of the RDF/XML object only has the system metadata information.
 * The solr docs of the science metadata doc and data file have the annotation information.
 */
public class RdfXmlSubprocessor implements IDocumentSubprocessor {

    private static Log log = LogFactory.getLog(RdfXmlSubprocessor.class);
    
    /**
     * If xpath returns true execute the processDocument Method
     */
    private List<String> matchDocuments = null;
    private List<ISolrDataField> fieldList = new ArrayList<ISolrDataField>();
    
    @Autowired
    private HTTPService httpService = null;

    @Autowired
    private String solrQueryUri = null;
    
    
    /**
     * Returns true if subprocessor should be run against object
     * 
     * @param formatId the the document to be processed
     * @return true if this processor can parse the formatId
     */
    public boolean canProcess(String formatId) {
        return matchDocuments.contains(formatId);
    } 
    
    public List<String> getMatchDocuments() {
        return matchDocuments;
    }

    public void setMatchDocuments(List<String> matchDocuments) {
        this.matchDocuments = matchDocuments;
    }
    public List<ISolrDataField> getFieldList() {
		return fieldList;
	}

	public void setFieldList(List<ISolrDataField> fieldList) {
		this.fieldList = fieldList;
	}

	/**
	 * Process an individual RDF/XML document, returning a map of processed documents to be indexed
	 * 
	 * @param identifier  the identifier of the document to process
	 * @param docs  a map of Solr documents keyed by identifier
	 * @param is  the input stream representation of the RDF/XML document to be processed
	 */
	@Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs, InputStream is) throws Exception {
        SolrDoc resourceMapDoc = docs.get(identifier);
        List<SolrDoc> processedDocs = process(resourceMapDoc, is);
        Map<String, SolrDoc> processedDocsMap = new HashMap<String, SolrDoc>();
        for (SolrDoc processedDoc : processedDocs) {
            processedDocsMap.put(processedDoc.getIdentifier(), processedDoc);
        }

        return processedDocsMap;
    }
    
	/* 
	 * Process triple statements found in an RDF/XML input stream document and return a list 
	 * of Solr documents to be indexed
	 */
    private List<SolrDoc> process(SolrDoc indexDocument, InputStream is) throws Exception {
    	
    	// get the triplestore dataset
		Dataset dataset = TripleStoreService.getInstance().getDataset();
		
    	// read the annotation
    	String indexDocId = indexDocument.getIdentifier();
    	String name = indexDocId;
    			
    	//Check if the identifier is a valid URI and if not, make it one by prepending "http://"
    	URI nameURI = new URI(indexDocId);
    	String scheme = nameURI.getScheme();
    	if((scheme == null) || (scheme.isEmpty())){
    		name = "http://" + indexDocId.toLowerCase();
    	}
    	
    	boolean loaded = dataset.containsNamedModel(name);
		if (!loaded) {
			OntModel ontModel = ModelFactory.createOntologyModel();
			ontModel.read(is, name);
			dataset.addNamedModel(name, ontModel);
		}
		//dataset.getDefaultModel().add(ontModel);
		
		// process each field query
        Map<String, SolrDoc> documentsToIndex = new HashMap<String, SolrDoc>();
		for (ISolrDataField field: this.fieldList) {
			String q = null;
			if (field instanceof SparqlField) {
				q = ((SparqlField) field).getQuery();
				q = q.replaceAll("\\$GRAPH_NAME", name);
				Query query = QueryFactory.create(q);
				QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
				ResultSet results = qexec.execSelect();
				
				while (results.hasNext()) {
					SolrDoc solrDoc = null;
					QuerySolution solution = results.next();
					System.out.println(solution.toString());
					
					// find the index document we are trying to augment with the annotation
					if (solution.contains("pid")) {
						String id = solution.getLiteral("pid").getString();
						
						// TODO: check if anyone with permissions on the annotation document has write permission on the document we are annotating
						boolean statementAuthorized = true;
						if (!statementAuthorized) {	
							continue;
						}
						
						// otherwise carry on with the indexing
						solrDoc = documentsToIndex.get(id);
						if (solrDoc == null) {
							solrDoc = new SolrDoc();
							solrDoc.addField(new SolrElementField(SolrElementField.FIELD_ID, id));
							documentsToIndex.put(id, solrDoc);
						}
					}

					// add the field to the index document
					if (solution.contains(field.getName())) {
						String value = solution.get(field.getName()).toString();
						SolrElementField f = new SolrElementField(field.getName(), value);
						if (!solrDoc.hasFieldWithValue(f.getName(), f.getValue())) {
							solrDoc.addField(f);
						}
					}
				}
			}
		}
		
		// clean up the triple store
		TDBFactory.release(dataset);

        return new ArrayList<SolrDoc>(documentsToIndex.values());
    }
    


    /**
     * Merge document updates with existing fields in the index
     * 
     * @param indexDocument  the document to index
     */
	@Override
	public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument)
			throws IOException, EncoderException, XPathExpressionException {
        
		SolrDoc solrDoc = httpService.retrieveDocumentFromSolrServer(indexDocument.getIdentifier(),
                solrQueryUri);
        if (solrDoc != null) {
            for (SolrElementField field : solrDoc.getFieldList()) {
                if ( !indexDocument.hasFieldWithValue(field.getName(), field.getValue()) ) {
                    indexDocument.addField(field);
                }
            }
        }
        
		return indexDocument;
	}


}
