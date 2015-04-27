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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;
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

	@Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs, InputStream is) throws Exception {
		if ( log.isTraceEnabled() ) {
			log.trace("INCOMING DOCS: ");
			for (SolrDoc doc : docs.values()) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				doc.serialize(baos, "UTF-8");
				log.trace(baos.toString());
			}
		}
		SolrDoc resourceMapDoc = docs.get(identifier);
        List<SolrDoc> processedDocs = process(resourceMapDoc, is);
        Map<String, SolrDoc> processedDocsMap = new HashMap<String, SolrDoc>();
        for (SolrDoc processedDoc : processedDocs) {
            processedDocsMap.put(processedDoc.getIdentifier(), processedDoc);
        }

		if ( log.isTraceEnabled() ) {
			log.trace("OUTGOING DOCS: ");
			for (SolrDoc doc : processedDocsMap.values()) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				doc.serialize(baos, "UTF-8");
				log.trace(baos.toString());
			}
		}

		// Merge previously processed (but yet to be indexed) documents
		Map<String, SolrDoc> mergedDocs = mergeDocs(docs, processedDocsMap);
		
        return mergedDocs;
    }
    
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
				log.trace("Executing SPARQL query:\n" + query.toString());
				QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
				ResultSet results = qexec.execSelect();
				while (results.hasNext()) {
					SolrDoc solrDoc = null;
					QuerySolution solution = results.next();
					log.trace(solution.toString());
					
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

		// merge the existing index with the new[er] values
        Map<String, SolrDoc> existingDocuments = getSolrDocs(documentsToIndex.keySet());
        Map<String, SolrDoc> mergedDocuments = mergeDocs(documentsToIndex, existingDocuments);
        mergedDocuments.put(indexDocument.getIdentifier(), indexDocument);
        
        return new ArrayList<SolrDoc>(mergedDocuments.values());
    }
    
    private Map<String, SolrDoc> getSolrDocs(Set<String> ids) throws Exception {
        Map<String, SolrDoc> list = new HashMap<String, SolrDoc>();
        if (ids != null) {
            for (String id : ids) {
            	SolrDoc doc = httpService.retrieveDocumentFromSolrServer(id, solrQueryUri);
                if (doc != null) {
                    list.put(id, doc);
                }
            }
        }
        return list;
    }
    
    /*
     * Merge existing documents from the Solr index with pending documents
     */
    private Map<String, SolrDoc> mergeDocs(Map<String, SolrDoc> pending, Map<String, SolrDoc> existing) throws Exception {

    	Map<String, SolrDoc> merged = new HashMap<String, SolrDoc>();
    	
    	Iterator<String> pendingIter = pending.keySet().iterator();
    	while (pendingIter.hasNext()) {
    		String id = pendingIter.next();
    		SolrDoc pendingDoc = pending.get(id);
    		SolrDoc existingDoc = existing.get(id);
    		SolrDoc mergedDoc = new SolrDoc();
    		if (existingDoc != null) {
    			// merge the existing fields
    			for (SolrElementField field: existingDoc.getFieldList()) {
    				mergedDoc.addField(field);
    				
    			}
    		}
    		// add the pending
    		for (SolrElementField field: pendingDoc.getFieldList()) {
    			if (field.getName().equals(SolrElementField.FIELD_ID) && mergedDoc.hasField(SolrElementField.FIELD_ID)) {
    				continue;
    			}
    			
				// only add if we don't already have it
				if (!mergedDoc.hasFieldWithValue(field.getName(), field.getValue())) {
					mergedDoc.addField(field);
				}	
			}
    		
    		// include in results
			merged.put(id, mergedDoc);
    	}
    	
		// add existing if not yet merged (needed if existing map size > pending map size)
    	Iterator<String> existingIter = existing.keySet().iterator();
    	
    	while ( existingIter.hasNext() ) {
    	    String existingId = existingIter.next();
    	    
    	    if ( ! merged.containsKey(existingId) ) {
    	        merged.put(existingId, existing.get(existingId));
    	        
    	    }
    	}
		
    	if (log.isTraceEnabled()) {
			log.trace("Merged docs with existing from the Solr index: ");
			for ( SolrDoc solrDoc : merged.values() ) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				solrDoc.serialize(baos, "UTF-8");
				log.trace("document to index: " + baos.toString());
			}
		}

    	return merged;
    }


	@Override
	public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument)
			throws IOException, EncoderException, XPathExpressionException {
    	
		log.trace("Looking up existing doc: " + indexDocument.getIdentifier() + " from: " + solrQueryUri);

        SolrDoc existingSolrDoc = httpService.retrieveDocumentFromSolrServer(indexDocument.getIdentifier(), solrQueryUri);
        if (existingSolrDoc != null) {
            for (SolrElementField field : indexDocument.getFieldList()) {
                log.debug("Checking new field: " + field.getName() +  "=" + field.getValue());
                
                // Temporary hack to deal with Solr handling of date formats as strings (00:00:00Z != 00:00:00.000Z)
                if (!existingSolrDoc.hasFieldWithValue(field.getName(), field.getValue())) {
                	
                	List<String> existingFieldValues = existingSolrDoc.getAllFieldValues(field.getName());
            		boolean foundExactDate = false;
                	Date solrDateTime = null;
					Date newDateTime = null;
                	for (String existingFieldValue : existingFieldValues) {
						try {
							solrDateTime = DatatypeConverter.parseDate(existingFieldValue).getTime();
							newDateTime = DatatypeConverter.parseDate(field.getValue()).getTime();
							
						} catch (Exception e) {
							// Not a parseable date, move on
							continue;
							
						}
						// The field value converts to a date, and matches an existing value as a date
                		if ( newDateTime.equals(solrDateTime) ) {
                			foundExactDate =  true;
                			break;
                			
                		}
            			
            		}
                	
                	// None of the existing fields match when converted to a date. Add it.
                	if ( ! foundExactDate ) {
                    	existingSolrDoc.addField(field);
                        log.debug("Adding new field/value to existing index doc " + 
                    	          existingSolrDoc.getIdentifier() + ": " + 
                        		  field.getName() +  "=" + field.getValue());
                		
                	}
                	
                } else {
                    log.debug("field name/value already exists in index: " + field.getName() +  "=" + field.getValue());
                    
                }
            }
            // return the augmented one that exists already
            return existingSolrDoc;
            
        } else {
        	log.warn("Could not locate existing document for: " + indexDocument.getIdentifier());
        	
        }
        return indexDocument;
	}


}