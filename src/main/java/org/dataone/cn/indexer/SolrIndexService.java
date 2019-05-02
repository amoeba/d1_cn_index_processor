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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.apache.solr.common.util.IOUtils;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.parser.IDocumentDeleteSubprocessor;
import org.dataone.cn.indexer.parser.IDocumentSubprocessor;
import org.dataone.cn.indexer.parser.SubprocessorUtility;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

/**
 * Top level document processing class.  
 * 
 * Contains collection of document sub-processors which are used to mine search
 * index data from document objects.  Each sub-processor is configured via spring
 * to collect data from different types of documents (by formatId).
 * 
 * There should only be one instance of this class in place at a time
 * since it performs updates on the SOLR index and transactions on SOLR are at
 * the server level - so if multiple threads write and commit then things could
 * get messy.
 * 
 */

public class SolrIndexService {

    private static Logger log = Logger.getLogger(SolrIndexService.class);
    private static final String OUTPUT_ENCODING = "UTF-8";
    private List<IDocumentSubprocessor> subprocessors = null;
    private List<IDocumentDeleteSubprocessor> deleteSubprocessors = null;
    private IDocumentSubprocessor systemMetadataProcessor = null;

    @Autowired
    private D1IndexerSolrClient d1IndexerSolrClient = null;

    @Autowired
    private String solrIndexUri = null;

    @Autowired
    private String solrQueryUri = null;
    
    @Autowired
    private SubprocessorUtility subprocessorUtility;

 //   @Autowired
    private boolean usePartialUpdate = Settings.getConfiguration().getBoolean("dataone.indexing.usePartialUpdate", false);
    
    private PerformanceLogger perfLog = PerformanceLogger.getInstance();
    
    public List<String> parseList = new ArrayList<String>();
    public List<Date> parseStartTimeList = new ArrayList<Date>();
    public List<Long> parseDurationList = new ArrayList<Long>();
    
    
    
    public SolrIndexService() {
    }

    /**
     * Removing a document from the index can involve changes to several related documents
     * 
     * @param identifier
     * @throws Exception
     */
    public void removeFromIndex(String identifier) throws Exception {

        Map<String, SolrDoc> docs = new HashMap<String, SolrDoc>();

        for (IDocumentDeleteSubprocessor deleteSubprocessor : getDeleteSubprocessors()) {
            if (log.isDebugEnabled()) {
                log.debug("... invoking processDocForDelete for identifier " + identifier + "[" + deleteSubprocessor.getClass().getSimpleName() + "/"
                        + deleteSubprocessor.getInstanceLabel() + "]");
            }
            docs.putAll(deleteSubprocessor.processDocForDelete(identifier, docs));
        }
        List<SolrDoc> docsToUpdate = new ArrayList<SolrDoc>();
        List<String> idsToIndex = new ArrayList<String>();
        for (String idToUpdate : docs.keySet()) {
            if (docs.get(idToUpdate) != null) {
                docsToUpdate.add(docs.get(idToUpdate));
                log.debug("...updating doc with id " + idToUpdate);
            } else {
                // update via reindex, as per IDocumentDeleteSubprocessor
                idsToIndex.add(idToUpdate);
                
            }
        }

        sendCommand(docsToUpdate);

        deleteDocFromIndex(identifier);

        for (String idToIndex : idsToIndex) {
            Identifier pid = new Identifier();
            pid.setValue(idToIndex);
            SystemMetadata sysMeta = HazelcastClientFactory.getSystemMetadataMap().get(pid);
            if (SolrDoc.visibleInIndex(sysMeta)) {
                String objectPath = HazelcastClientFactory.getObjectPathMap().get(pid);
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                TypeMarshaller.marshalTypeToOutputStream(sysMeta, os);
                
                insertIntoIndex(idToIndex, new ByteArrayInputStream(os.toByteArray()), objectPath);
                log.debug("...reindexing doc with id " + idToIndex);
            }
        }
    }

    public void removeFromIndex(List<IndexTask> tasks) throws Exception {
    
        for (IndexTask indexTask : tasks) {
            String identifier = indexTask.getPid();
            try {
                removeFromIndex(identifier);
            } catch (Exception e) {
                log.error("Unable to remove from index: " + identifier, e);
            }
        }
        
        /*
         * TODO test:
        
        SolrElementAdd addCommand = new SolrElementAdd();
        List<String> identifiersToDelete = new ArrayList<String>();
        List<String> idsToIndex = new ArrayList<String>();
        List<IndexTask> tasksToIndex = new ArrayList<IndexTask>();
        
        for (IndexTask indexTask : tasks) {
            Map<String, SolrDoc> docs = new HashMap<String, SolrDoc>();
            String identifier = indexTask.getPid();
            
            for (IDocumentDeleteSubprocessor deleteSubprocessor : getDeleteSubprocessors()) {
                docs.putAll(deleteSubprocessor.processDocForDelete(identifier, docs));
            }
            List<SolrDoc> docsToUpdate = new ArrayList<SolrDoc>();
            
            for (String idToUpdate : docs.keySet()) {
                if (docs.get(idToUpdate) != null) {
                    docsToUpdate.add(docs.get(idToUpdate));
                } else {
                    idsToIndex.add(idToUpdate);
                }
            }

            SolrElementAdd newAddCommand = getAddCommand(docsToUpdate);
            addCommand = mergeAddCommands(addCommand, newAddCommand);
            
            identifiersToDelete.add(identifier);

            for (String idToIndex : idsToIndex) {
                Identifier pid = new Identifier();
                pid.setValue(idToIndex);
                SystemMetadata sysMeta = HazelcastClientFactory.getSystemMetadataMap().get(pid);
                if (SolrDoc.visibleInIndex(sysMeta)) {
                    String objectPath = HazelcastClientFactory.getObjectPathMap().get(pid);
                    IndexTask task = new IndexTask(sysMeta, objectPath);
                    tasksToIndex.add(task);
                }
            }
        }
        
        sendCommand(addCommand);
        d1IndexerSolrClient.sendSolrDeletes(identifiersToDelete);
        insertIntoIndex(tasksToIndex);
        */
    }
    
 
    
    /**
     * assemble a map of solrDocs by identifier, built by the sub-processors that 'canProcess' the object's format
     * Because of relationship fields in Annotation and ResourceMaps, more than one SolrDoc can be in the returned map.
     * The SolrDocs represent a compendium of all field information extracted by sub-processors.
     * @param id
     * @param systemMetaDataStream
     * @param objectPath
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public Map<String, SolrDoc> parseTaskObject(String id, InputStream systemMetaDataStream,
            String objectPath) throws IOException, SAXException, ParserConfigurationException,
            XPathExpressionException, EncoderException {

        
        Map<String, SolrDoc> accumulatedParsedFieldValues = new HashMap<String, SolrDoc>();
        try {
            long sysmetaProcStart = System.currentTimeMillis();
            parseList.add("id : SysMetaProcessor");
            Date d = new Date();
            parseStartTimeList.add(d);
            accumulatedParsedFieldValues = systemMetadataProcessor.processDocument(id, accumulatedParsedFieldValues, systemMetaDataStream);
            parseDurationList.add(System.currentTimeMillis() - d.getTime());
            if (log.isDebugEnabled()) 
                log.debug("...subprocessor SystemMetadataProcessor invoked for " + id);
            perfLog.log(systemMetadataProcessor.getClass().getSimpleName() + ".processDocument() processing sysmeta for id "+id, System.currentTimeMillis() - sysmetaProcStart);
        } catch (Exception e) {
            log.error("Error parsing system metadata for id: " + id + e.getMessage(), e);
        }

        if (objectPath != null) {
            SolrDoc sDoc = accumulatedParsedFieldValues.get(id);            
            String formatId = sDoc.getFirstFieldValue(SolrElementField.FIELD_OBJECTFORMAT);
            int i=1;
            for (IDocumentSubprocessor subprocessor : getSubprocessors()) {
                if (subprocessor.canProcess(formatId)) {
                    FileInputStream objectStream = null;
                    try {
                        if (log.isDebugEnabled()) 
                            log.debug("...subprocessor " + subprocessor.getClass().getSimpleName() + ".processDocument(...) invoked for " + id);
                        
                        // note that resource map processing touches all objects
                        // referenced by the resource map.
                        long startFechingFile = System.currentTimeMillis();
                        objectStream = new FileInputStream(objectPath);
                        perfLog.log("Loop "+i+". SolrIndexService.processObject() fetch file for id "+id, System.currentTimeMillis() - startFechingFile);
                        if (!objectStream.getFD().valid()) {
                            log.error("Could not load OBJECT file for ID,Path=" + id + ", "
                                    + objectPath);
                        } else {
                            parseList.add("id : " + subprocessor.getClass().getSimpleName());
                            Date dd = new Date();
                            parseStartTimeList.add(dd);
                            long scimetaProcStart = System.currentTimeMillis();
                            accumulatedParsedFieldValues = subprocessor.processDocument(id, accumulatedParsedFieldValues, objectStream);
                            parseDurationList.add(System.currentTimeMillis() - dd.getTime());
                            perfLog.log(String.format(
                                    "Loop %d. SolrIndexService.processObject() " 
                                            + "%s.processDocument() total subprocessor processing time for id %s with format: %s",
                                            i, 
                                            subprocessor.getClass().getSimpleName(),
                                            id,
                                            formatId),
                                            System.currentTimeMillis() - scimetaProcStart);
                        }
                    } catch (Exception e) {
                        log.warn(String.format("The subprocessor %s can't process the id %s since %s. " +
                                "However, the index still can be achieved without this part of information provided by the processor.",
                                subprocessor.getClass().getName(),
                                id,
                                e.getMessage()),
                                e);
                    }  finally {
                       IOUtils.closeQuietly(objectStream);
                    }
                }
                i++;
            }
        } else {
            log.warn("The optional objectPath for pid " + id + " is null, so skipping processing with content subprocessors");
        }
        return accumulatedParsedFieldValues;
    }
 
    /**
     * Given a PID, system metadata input stream, and an optional document
     * path, populate the set of SOLR fields for the document. 
     * 
     * @param id
     * @param systemMetaDataStream
     * @param objectPath
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public List<SolrDoc> processObject(String id, InputStream systemMetaDataStream,
            String objectPath) throws IOException, SAXException, ParserConfigurationException,
            XPathExpressionException, EncoderException { 

        long processObjStart = System.currentTimeMillis();
        
        Map<String, SolrDoc> docs = parseTaskObject(id, systemMetaDataStream, objectPath);
        log.debug(docs.size() + " SolrDocs parsed for id '" + id + "'");
        long mergeProcStart = System.currentTimeMillis();
        Map<String, SolrDoc> mergedDocs = new HashMap<String, SolrDoc>();
        int index =1;

        for (SolrDoc mergeDoc : docs.values()) {
            int innerIndex =1;
            if (this.usePartialUpdate) {
                log.debug("...SubprocessorUtility.diffWithIndexedDocument(...) to be invoked for " + mergeDoc.getIdentifier());
                long before = System.currentTimeMillis();
                mergeDoc = this.subprocessorUtility.diffWithIndexedDocument(mergeDoc);
                perfLog.log("Outer loop "+index+", inner loop"+innerIndex+" SolrIndexService.processObject() merging (by diff) docs for id "+id, System.currentTimeMillis() - before);
                mergedDocs.put(mergeDoc.getIdentifier(), mergeDoc); 
            }
            else {
                for (IDocumentSubprocessor subprocessor : getSubprocessors()) {
                    log.debug("...subprocessor " + subprocessor.getClass().getSimpleName() + ".mergeWithIndexedDocument(...) invoked for " + mergeDoc.getIdentifier());
                    long before = System.currentTimeMillis();
                    mergeDoc = subprocessor.mergeWithIndexedDocument(mergeDoc);
                    perfLog.log("Outer loop "+index+", inner loop"+innerIndex+" SolrIndexService.processObject() merging docs for id "+id, System.currentTimeMillis() - before);
                    innerIndex++;
                }
                mergedDocs.put(mergeDoc.getIdentifier(), mergeDoc);
                index++;
            }
        }
        perfLog.log("Total - SolrIndexService.processObject() merging docs for id "+id, System.currentTimeMillis() - mergeProcStart);
        perfLog.log("SolrIndexService.processObject() total processing time for id " + id, System.currentTimeMillis() - processObjStart);
        
        // the additions
        return new ArrayList<SolrDoc>(mergedDocs.values());
    }

    /**
     * Given a PID, system metadata input stream, and an optional document
     * path, populate the set of SOLR fields for the document and update the
     * index. Note that if the document is a resource map, then records that it
     * references will be updated as well.
     * 
     * @param id
     * @param systemMetaDataStream
     * @param objectPath
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public void insertIntoIndex(String id, InputStream systemMetaDataStream, String objectPath)
            throws IOException, SAXException, ParserConfigurationException,
            XPathExpressionException, EncoderException, Exception {

        
        // get the add command for solr
        List<SolrDoc> addCommands = processObject(id, systemMetaDataStream, objectPath);

        // send it
        long solrAddStart = System.currentTimeMillis();
        sendCommand(addCommands);
        perfLog.log("SolrIndexService.sendCommand(SolrElementAdd) adding docs into Solr index for id "+id, System.currentTimeMillis() - solrAddStart);
    }

    public void insertIntoIndex(List<IndexTask> tasks) 
            throws IOException, SAXException, ParserConfigurationException,
            XPathExpressionException, EncoderException, Exception {
     
        List<SolrDoc> batchAddCommands = new ArrayList<SolrDoc>();
        
        for (IndexTask task : tasks) {
            InputStream smdStream = new ByteArrayInputStream(task.getSysMetadata().getBytes());
            
            // get the add command for solr
            List<SolrDoc> addCommand = processObject(task.getPid(), smdStream, task.getObjectPath());
            batchAddCommands = mergeAddCommands(batchAddCommands, addCommand);
        }
        // send batch add command
        long solrAddStart = System.currentTimeMillis();
        sendCommand(batchAddCommands);
        perfLog.log("SolrIndexService.sendCommand(SolrElementAdd) batch adding (" + tasks.size() + ") docs into Solr index", System.currentTimeMillis() - solrAddStart);
    }
    
    private List<SolrDoc> mergeAddCommands(List<SolrDoc> original, List<SolrDoc> addition) {
        if (original == null)
            original = new ArrayList<SolrDoc>();
        if (addition == null)
            addition = new ArrayList<SolrDoc>();
        
        original.addAll(addition);
        
        return original;
    }
    
    private void sendCommand(List<SolrDoc> addCommands) throws IOException {
        D1IndexerSolrClient service = getD1IndexerSolrClient();
        log.info("sendCommand using partial update: " + this.usePartialUpdate);
        service.sendUpdate(getSolrindexUri(), addCommands, OUTPUT_ENCODING, this.usePartialUpdate);
    }


    private void deleteDocFromIndex(String identifier) {
        d1IndexerSolrClient.sendSolrDelete(identifier);
    }

    public String getSolrindexUri() {
        return solrIndexUri;
    }

    public void setSolrIndexUri(String solrindexUri) {
        this.solrIndexUri = solrindexUri;
    }

    public void setD1IndexerSolrClient(D1IndexerSolrClient service) {
        this.d1IndexerSolrClient = service;
    }

    public D1IndexerSolrClient getD1IndexerSolrClient() {
        return d1IndexerSolrClient;
    }

    public String getSolrQueryUri() {
        return solrQueryUri;
    }

    public void setSolrQueryUri(String solrQueryUri) {
        this.solrQueryUri = solrQueryUri;
    }

    public List<IDocumentSubprocessor> getSubprocessors() {
        if (this.subprocessors == null) {
            this.subprocessors = new ArrayList<IDocumentSubprocessor>();
        }
        return subprocessors;
    }

    public List<IDocumentDeleteSubprocessor> getDeleteSubprocessors() {
        if (this.deleteSubprocessors == null) {
            this.deleteSubprocessors = new ArrayList<IDocumentDeleteSubprocessor>();
        }
        return deleteSubprocessors;
    }

    public void setSubprocessors(List<IDocumentSubprocessor> subprocessorList) {
        this.subprocessors = subprocessorList;
    }

    public void setDeleteSubprocessors(List<IDocumentDeleteSubprocessor> deleteSubprocessorList) {
        this.deleteSubprocessors = deleteSubprocessorList;
    }

    public IDocumentSubprocessor getSystemMetadataProcessor() {
        return systemMetadataProcessor;
    }

    public void setSystemMetadataProcessor(IDocumentSubprocessor systemMetadataProcessor) {
        this.systemMetadataProcessor = systemMetadataProcessor;
    }

    public void setUsePartialUpdate(boolean value) {
        this.usePartialUpdate = value;
    }
    
    public boolean getUsePartialUpdate() {
        return this.usePartialUpdate;
    }
}
