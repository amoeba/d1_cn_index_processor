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

package org.dataone.cn.indexer;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.parser.IDocumentSubprocessorV2;
import org.dataone.cn.indexer.parser.UpdateAssembler;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public abstract class AbstractStubMergingSubprocessor implements IDocumentSubprocessorV2 {

    private PerformanceLogger perfLog = PerformanceLogger.getInstance();
    private static Logger log = Logger.getLogger(AbstractStubMergingSubprocessor.class);
   
    @Autowired
    @Qualifier("solrQueryUri")
    protected String solrQueryUri; 
    
    @Autowired
    protected D1IndexerSolrClient d1IndexerSolrClient;
    
    public AbstractStubMergingSubprocessor() {
    }
    
    /**
     * Implementations parse the inputStream and collect solr fields into the mainDocRef and the otherDocsRef.
     * 
     * @param source - source inputstream representing the object bytes to be parsed
     * @param mainDocRef - the SolrDoc from upstream processors for the index task 
     * @param otherDocsRef - the map of associated document to which will be added new items
     */
    protected abstract Map<String,SolrDoc> parseDocument(String mainIdentifier, InputStream source)
     throws Exception;
    

    /**
     * The standard method for processing the input document.  Calls the abstract parseDocument
     * method and then 
     *  
     */
    @Override
    public void processDocument(String identifier, UpdateAssembler collector, InputStream is)
            throws Exception {
    
        long processStart = System.currentTimeMillis();

        Map<String,SolrDoc> docMap = parseDocument(identifier, is);
        log.info("number of documents from parseDocuments: " + docMap.size());
        
//        for (Map.Entry<String,SolrDoc> parsedDoc : docMap.entrySet()) {
//            System.out.println("============================================================");
//            System.out.println("parsedDoc: " + parsedDoc.getKey());
//            System.out.println("    id: " + parsedDoc.getValue().getFirstFieldValue("id"));
//            System.out.println("   pid: " + parsedDoc.getValue().getFirstFieldValue("identifier"));
//            System.out.println("   sid: " + parsedDoc.getValue().getFirstFieldValue("seriesId"));
//            System.out.println("     v: " + parsedDoc.getValue().getFirstFieldValue("_version_"));
//            System.out.println("============================================================");
//        }
        perfLog.log("AbstractMultiDocSubProcessor.parseDocument() for id "+identifier, System.currentTimeMillis() - processStart);

        // work with the main document if it's there 
        // 
        SolrDoc mainDoc = docMap.get(identifier);
        log.info(" main document from parseDocuments: " + mainDoc);
        if (mainDoc != null) {
            log.info(" main document id from parseDocuments: " + mainDoc.getIdentifier());
            collector.addToUpdate(identifier, null, mainDoc);        

            
            // this next part will run mainly by the systemMetadata subprocessor 
            // (other subprocessors don't populate the seriesId field...)
            // and will pull in relationships stored in SID-identified stubs into the main record.
            
            String seriesId = mainDoc.getSeriesId();       
            if (seriesId != null) {
                List<SolrDoc> existingSidStubs = d1IndexerSolrClient.getDocumentBySolrId(solrQueryUri,seriesId);
                
                if (existingSidStubs != null && !existingSidStubs.isEmpty()) {
                    SolrDoc newRelationships = existingSidStubs.get(0); // there can be only one
                    SolrElementField idField = newRelationships.getField("id");
                    String mainIdentifier = mainDoc.getIdentifier();
                    idField.setValue(mainIdentifier);
                    collector.addToUpdate(mainIdentifier, null, newRelationships);
                }
            }
        }

        // now prepare the stub docs
        // by querying for existing solr records by ID and seriesId
        
        docMap.remove(identifier);  // the remaining docs are stub documents
        
        if (!docMap.isEmpty()) {
            for (Entry<String, SolrDoc> n: docMap.entrySet()) {
                
                String id = n.getKey();               
                SolrDoc parsedStub = n.getValue();
                
                log.info(" other document to process: " + id );
                
                List<SolrDoc> otherExisting = d1IndexerSolrClient.getDocumentBySolrId(solrQueryUri, id);
 
                
                if (otherExisting != null && !otherExisting.isEmpty()) {
                    log.info("found existing record for the stub, id = " + id);
                    log.info("    .... version is: " + otherExisting.get(0).getFirstFieldValue("_version_"));
                    collector.addToUpdate(id, otherExisting.get(0), parsedStub);  // will only be 1 of them 

                } else {
                    // id is either seriesID or unknown, so need to make the stub record either way
                    collector.addToUpdate(id, null, parsedStub);

                    // now lookup by series id for possible incorporation
                    List<SolrDoc> associatedExistingDocs = d1IndexerSolrClient.getDocumentsByField(solrQueryUri, Collections.singletonList(id), 
                            SolrElementField.FIELD_SERIES_ID, true);
                    if (associatedExistingDocs != null) {
                        for (SolrDoc assocExisting: associatedExistingDocs) {
                            String associatedPid = assocExisting.getIdentifier();
                            log.info("found existing records for the stub by seriesId.  existing pid = " + associatedPid);
                            SolrDoc stubToAdd = parsedStub.clone();
                            stubToAdd.getField("id").setValue(associatedPid);  // sets the id field with the PID
                            collector.addToUpdate(associatedPid, assocExisting, stubToAdd);
                        }
                    }
                }
            }
        }
        perfLog.log("AbstractMultiDocSubProcessor.processDocument() for id "+identifier, System.currentTimeMillis() - processStart);
    }
}
