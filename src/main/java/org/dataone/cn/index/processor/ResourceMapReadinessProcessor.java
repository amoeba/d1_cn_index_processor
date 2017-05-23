package org.dataone.cn.index.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.task.ResourceMapIndexTask;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceMapFactory;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.cn.messaging.QueueAccess;
import org.dataone.service.types.v2.SystemMetadata;
import org.dspace.foresite.OREParserException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;



public class ResourceMapReadinessProcessor  {

    static final Logger logger = Logger.getLogger(ResourceMapReadinessProcessor.class);
    private CachingConnectionFactory connFact = new CachingConnectionFactory();
    
    public final static String RESOURCE_MAP_QUEUE_NAME = "indexing.resmap_waiting_for_readiness";
    public final static String READY_TO_PROCESS_QUEUE_NAME = "indexing.ready_to_process";
    public final static String FAILED_RESMAP_READINESS_PROCESSING = "indexing.failed_resmap_readiness_processing";

    private QueueAccess sourceQueue;
    
    @Autowired
    private HTTPService httpService;
    
    @Autowired
    private String solrQueryUri;
    
    
    public ResourceMapReadinessProcessor(QueueAccess sourceQueue) {
        this.sourceQueue = sourceQueue;
    }

    /**
     * This processor determines if the task is ready to be indexed or needs to
     * wait, based on how many of its referenced Ids have not been indexed.
     * 
     * If it needs to wait, it removes from the task's referencedIds list those
     * identifiers that have been determined to be indexed / archived / or deleted,
     * to avoid redundant querying.
     * 
     * Those tasks that have to wait go back onto source queue .
     * @throws IOException 
     * 
     */
    public boolean processHandler(Message message) throws IOException  {
 
        QueueAccess destinationQueue;
        ResourceMapIndexTask rmit;
        
        try {
            rmit = ResourceMapIndexTask.deserialize(message.getBody());
        } catch (IOException | ClassNotFoundException e) {
            destinationQueue = new QueueAccess(connFact, FAILED_RESMAP_READINESS_PROCESSING);
            destinationQueue.publish(message);
            return true;
        }
        
        
        boolean ready = false;
        destinationQueue = null;
        List<String> waitingForIds = null;
        
        try {

            // see if it's already indexed
            // if so, then no need to delay re-indexing
            List<SolrDoc> resMap = httpService.getDocumentById(this.solrQueryUri, rmit.getPid());
 
            
            if (resMap != null && resMap.size() > 0) {
                ready = true;
                destinationQueue = new QueueAccess(connFact, READY_TO_PROCESS_QUEUE_NAME);

                
            } else {

                
                waitingForIds =  extractReferencedIds(rmit);

                // find by ID in the index, and filter the list
                List<SolrDoc> foundSolrDocs = httpService.getDocumentsByField(this.solrQueryUri, waitingForIds,
                        SolrElementField.FIELD_ID, false);
                ArrayList<String> foundInIndex = new ArrayList<>();
                for (SolrDoc doc : foundSolrDocs) {
                    foundInIndex.add(doc.getIdentifier());
                }
                waitingForIds.removeAll(foundInIndex);
                if (waitingForIds.isEmpty()) {
                    ready = true;

 
                } else {


                    // find by SERIES_ID in the index, and filter the list
                    
                    foundSolrDocs = httpService.getDocumentsByField(this.solrQueryUri, waitingForIds,
                            SolrElementField.FIELD_SERIES_ID, false);
                    foundInIndex.clear();
                    for (SolrDoc doc : foundSolrDocs) {
                        foundInIndex.add(doc.getSeriesId());
                    }
                    waitingForIds.removeAll(foundInIndex); 
                    if (waitingForIds.isEmpty()) {
                        ready = true;

                        
                    } else {

                        
                        // check to see if any are archived or deleted (not supposed to be in the index)
                        
                        for (Iterator<String> it = waitingForIds.iterator(); it.hasNext();) {
                            String id = it.next();
                            SystemMetadata smd = HazelcastClientFactory.getSystemMetadataMap().get(id);
                            if (smd != null && ! SolrDoc.visibleInIndex(smd)) {
                                it.remove();
                            }
                        }
                    }
                }
            }
            
            
           
        } catch (XPathExpressionException | EncoderException e) {
            destinationQueue = new QueueAccess(connFact, FAILED_RESMAP_READINESS_PROCESSING);
            logger.error("Unable to query solr for identifiers for task : " + rmit.getPid()
                    + ".  Unrecoverable error: task to be moved to " + FAILED_RESMAP_READINESS_PROCESSING);
            if (logger.isTraceEnabled()) {
                e.printStackTrace();
            }
        } catch (OREParserException e) {
            destinationQueue = new QueueAccess(connFact, FAILED_RESMAP_READINESS_PROCESSING);
            logger.error("Unable to parse ORE doc: " + rmit.getPid()
                    + ".  Unrecoverable parse error: task moved to " + FAILED_RESMAP_READINESS_PROCESSING);
            if (logger.isTraceEnabled()) {
                e.printStackTrace();
            }
        }
        
        
        if (ready) {
            destinationQueue = new QueueAccess(connFact, READY_TO_PROCESS_QUEUE_NAME);
        } else {
            // it's not ready to put to the end of starting queue
            if (destinationQueue == null) {
                // TODO: set delay
                destinationQueue = this.sourceQueue;
            } else {
                ; // there was an unrecoverable error
            }
        }
        
        
        // set the winnowed referenced id list for the next time.
        
        Message resultingMessage = null;
        try {
            rmit.setReferencedIds(waitingForIds);
            resultingMessage = new Message(rmit.serialize(), message.getMessageProperties());
        } catch (IOException e) {
            // this IOException has to do with message serialization, so, unlike the others
            // that are thrown by this method, is unrecoverable
            logger.error("Unable to serialize the task: " + rmit.getPid()
                    + ".  Unrecoverable error, so task moved to " + FAILED_RESMAP_READINESS_PROCESSING);
            if (logger.isTraceEnabled()) {
                e.printStackTrace();
            }
            destinationQueue = new QueueAccess(connFact, FAILED_RESMAP_READINESS_PROCESSING);
            // have to use the original message
            destinationQueue.publish(message);
        }
        destinationQueue.publish(resultingMessage);
        return true;
    }
 
    
    /*
     * retrieves or builds the referencedId list for the task.
     */
    private List<String>  extractReferencedIds(ResourceMapIndexTask t) throws OREParserException {

        if (t.getReferencedIds() == null || t.getReferencedIds().isEmpty()) {
            // it hasn't been parsed yet.
            ResourceMap rm = ResourceMapFactory.buildResourceMap(t.getObjectPath());
            List<String> allReferenced = rm.getAllDocumentIDs();

            // remove the resmap id itself from the getAllDocumentIDs list
            boolean found = allReferenced.remove(t.getPid());
            while(found) 
                found = allReferenced.remove(t.getPid());
            
            return allReferenced;
        } 
        else {
            return t.getReferencedIds();
        }
    }
}
