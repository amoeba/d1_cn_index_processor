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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.processor.IndexTaskDeleteProcessor;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.parser.utility.SeriesIdResolver;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceMapFactory;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.SystemMetadata;
import org.dspace.foresite.OREParserException;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * Resource Map Document processor.  Operates on ORE/RDF objects.  Maps 
 * 'documents', 'documentedBy', and 'aggregates' relationships.
 * 
 * Uses org.dataone.cn.indexer.resourcemap.ResourceMap to update individual
 * SolrDoc objects with values for 'documents', 'documentedBy', and 'resourceMap'
 * (aggregates) fields.
 * 
 * Updates entries for related documents in index. For document relational
 * information refer to
 * http://purl.dataone.org/architecture/design/SearchMetadata.html#id4
 * 
 * Date: 9/26/11
 * Time: 3:51 PM
 */
public class ResourceMapSubprocessor implements IDocumentSubprocessor {

    private static Logger logger = Logger.getLogger(ResourceMapSubprocessor.class.getName());

    @Autowired
    private D1IndexerSolrClient d1IndexerSolrClient = null;

    @Autowired
    private String solrQueryUri = null;

    @Autowired
    private IndexTaskDeleteProcessor deleteProcessor;

    @Autowired
    private SubprocessorUtility processorUtility;

    private PerformanceLogger perfLog = PerformanceLogger.getInstance();
    
    private List<String> matchDocuments = null;
    private List<String> fieldsToMerge = new ArrayList<String>();

    /**
     * Merge updates with existing solr documents
     * 
     * @param indexDocument
     * @return
     * @throws IOException
     * @throws EncoderException
     * @throws XPathExpressionException
     */
    @Override
    public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument) throws IOException,
            EncoderException, XPathExpressionException {
        return processorUtility.mergeWithIndexedDocument(indexDocument, fieldsToMerge);
    }

    /**
     * Implements IDocumentSubprocessor.processDocument method.
     * Creates a map of D1 Identifier string values to SolrDoc objects which represent
     * the Solr search index documents for the data package defined by the ORE/RDF document.
     * Each solr record in the data package is updated with 'documents', 'documentedBy',
     * and 'resourceMap' field values contained in the incoming Document doc method parameter.
     */
    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            InputStream is) throws XPathExpressionException, IOException, EncoderException {
        SolrDoc resourceMapSolrDoc = docs.get(identifier);
        List<SolrDoc> processedDocs = new ArrayList<SolrDoc>();
        try {            
            long procResMapStart = System.currentTimeMillis();
            processedDocs = processResourceMap(resourceMapSolrDoc, is);
            perfLog.log("ResourceMapSubprocessor.processDocument() for id "+identifier, System.currentTimeMillis() - procResMapStart);
        } catch (OREParserException oreException) {
            logger.error("Unable to parse resource map: " + identifier
                    + ".  Unrecoverable parse exception:  task will not be re-tried.");
        }
        Map<String, SolrDoc> processedDocsMap = new HashMap<String, SolrDoc>();
        for (SolrDoc processedDoc : processedDocs) {
            processedDocsMap.put(processedDoc.getIdentifier(), processedDoc);
        }
        return processedDocsMap;
    }

    /**
     * Given the starting SolrDoc for the resourcemap (from upstream processors), and parsed XML,
     * get all the members,  
     */
     protected List<SolrDoc> processResourceMap(SolrDoc indexDocument, InputStream resourceMapStream)
            throws OREParserException, XPathExpressionException, IOException, EncoderException {

        long buildResMapStart = System.currentTimeMillis();
          
        // this new way of building the solr records doesn't yet deal with seriesId in the relationship fields, 
        // or calling clearObsoletesChain to do whatever it is that it's supposed to do...
        
        Map<Identifier, Map<Identifier, List<Identifier>>> tmpResourceMap = null;

        try {
            tmpResourceMap = org.dataone.ore.ResourceMapFactory.getInstance().parseResourceMap(resourceMapStream);

        } catch (Throwable e) {
            logger.error("Unable to parse ORE document:", e);
            throw new OREParserException(e);
        }
        perfLog.log("ResourceMapFactory.buildResourceMap() create ResourceMap from InputStream", System.currentTimeMillis() - buildResMapStart);
        
        Map<Identifier,SolrDoc> memberDocs = new HashMap<>();
        Entry<Identifier, Map<Identifier, List<Identifier>>> resMapHierarchy = tmpResourceMap.entrySet().iterator().next();
        String resourceMapId = resMapHierarchy.getKey().getValue();
        Map<Identifier,List<Identifier>> metadataMap = resMapHierarchy.getValue();
        for (Identifier mdId : metadataMap.keySet()) {
            if (!memberDocs.containsKey(mdId)) {
                memberDocs.put(mdId,new SolrDoc());
                memberDocs.get(mdId).addField(new SolrElementField("id",mdId.getValue()));
                memberDocs.get(mdId).addField(new SolrElementField("resourceMap",resourceMapId));
            }
            for (Identifier dataId : metadataMap.get(mdId)) {
                if (!memberDocs.containsKey(dataId)) {
                    memberDocs.put(dataId, new SolrDoc());
                    memberDocs.get(dataId).addField(new SolrElementField("id",dataId.getValue()));
                    memberDocs.get(dataId).addField(new SolrElementField("resourceMap",resourceMapId));
                }
                memberDocs.get(dataId).addField(new SolrElementField("isDocumentedBy",mdId.getValue()));
                memberDocs.get(mdId).addField(new SolrElementField("documents",dataId.getValue()));

            }
        }
        List<SolrDoc> allMembers = new ArrayList<>();
        allMembers.add(indexDocument);
        allMembers.addAll(memberDocs.values());
        return allMembers;
        
/////    The old way of parsing resource maps
//        
//        ResourceMap resourceMap = ResourceMapFactory.buildResourceMap(resourceMapStream);
//        perfLog.log("ResourceMapFactory.buildResourceMap() create ResourceMap from InputStream", System.currentTimeMillis() - buildResMapStart);
//        
//        long getReferencedStart = System.currentTimeMillis();
//        List<String> memberIds = resourceMap.getAllDocumentIDs();     // all members of the package, including the resource map
//        perfLog.log("ResourceMap.getAllDocumentIDs() referenced in ResourceMap", System.currentTimeMillis() - getReferencedStart);
//        
//        long clearObsoletesChainStart = System.currentTimeMillis();
//        this.clearObsoletesChain(indexDocument.getIdentifier(), memberIds);
//        perfLog.log("ResourceMapSubprocessor.clearObosoletesChain() removing obsoletes chain from Solr index", System.currentTimeMillis() - clearObsoletesChainStart);
//        
//        // create solrDocs for data members
//        // flesh-out the member solr documents from solr
//        long getSolrDocsStart = System.currentTimeMillis();
//        List<SolrDoc> memberUpdateDocuments = d1IndexerSolrClient.getDocumentsByD1Identifier(solrQueryUri, memberIds);
//        perfLog.log("d1IndexerSolrClient.getDocumentsById() get existing referenced ids' Solr docs", System.currentTimeMillis() - getSolrDocsStart);
//        
//        List<SolrDoc> mergedDocuments = resourceMap.mergeIndexedDocuments(memberUpdateDocuments);
//        mergedDocuments.add(indexDocument);
//        return mergedDocuments;
    }

    /**
     * Deletes the resourceMap and its obsoletes chain from the search index
     * if any of the package members was identified by seriesId.
     * (why?)
     */
    private void clearObsoletesChain(String resourceMapIdentifier, List<String> memberIds) {

        // check that we are, indeed, dealing with a SID-identified ORE

        boolean memberListContainsSeriesId = false;
        for (String memberId : memberIds) {
            Identifier relatedPid = new Identifier();
            relatedPid.setValue(memberId);
            if (SeriesIdResolver.isSeriesId(relatedPid)) {
                memberListContainsSeriesId = true;
                break;
            }
        }
        // if this package contains a SID, then we need to clear out old info
        // (I believe this means that if there's a SID in the member list, we should remove
        // the resourceMap 
        if (memberListContainsSeriesId) {
            Identifier pidToProcess = TypeFactory.buildIdentifier(resourceMapIdentifier);
            while (pidToProcess != null) {
                // queue a delete processing of all versions in the obsoletes chain
                SystemMetadata sysmeta = HazelcastClientFactory.getSystemMetadataMap().get(
                        pidToProcess);
                if (sysmeta != null) {
                    String objectPath = HazelcastClientFactory.getObjectPathMap().get(pidToProcess);
                    logger.debug("Removing pidToProcess===" + pidToProcess.getValue());
                    logger.debug("Removing objectPath===" + objectPath);

                    
                    IndexTask task = new IndexTask(sysmeta, objectPath);
                    try {
                        deleteProcessor.process(task);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    // go to the next one
                    pidToProcess = sysmeta.getObsoletes();
                }
                else {
                    pidToProcess = null;
                }
            }
        }
    }

    public List<String> getMatchDocuments() {
        return matchDocuments;
    }

    public void setMatchDocuments(List<String> matchDocuments) {
        this.matchDocuments = matchDocuments;
    }

    public boolean canProcess(String formatId) {
        return matchDocuments.contains(formatId);
    }

    public List<String> getFieldsToMerge() {
        return fieldsToMerge;
    }

    public void setFieldsToMerge(List<String> fieldsToMerge) {
        this.fieldsToMerge = fieldsToMerge;
    }
}
