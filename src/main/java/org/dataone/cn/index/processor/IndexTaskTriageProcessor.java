package org.dataone.cn.index.processor;

import java.util.Map;

import org.dataone.cn.messaging.QueueAccess;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

public class IndexTaskTriageProcessor {

    private CachingConnectionFactory connFact = new CachingConnectionFactory();
    
    public final static String RESOURCE_MAP_QUEUE_NAME = "indexing.resmap_waiting_for_readiness";
    public final static String READY_TO_PROCESS_QUEUE_NAME = "indexing.ready_to_process";
    public final static String FAILED_TRIAGE_QUEUE_NAME = "indexing.failed_triage";
    public final static int PRIORITY_RANGE = 5;
    public final static int PRIORITY_LOOKBACK = 100;
    
    private QueuePrioritizer qp;
    
    public IndexTaskTriageProcessor(QueueAccess sourceQueue) {
        qp = new QueuePrioritizer(PRIORITY_LOOKBACK,PRIORITY_RANGE);
    }

    /**
     * this method assigns a relative priority by source nodeId
     * and also splits the input into two streams, one for resourceMaps, and the other for the rest 
     */

    public boolean processHandler(Message message) throws Exception {
 
        Map<String,Object> msgHeaders = message.getMessageProperties().getHeaders();

        //determine the relative priority based on nodeId message header 
        float priority = 1.0f;
        if (msgHeaders.containsKey("nodeId")) {
            priority = qp.pushNext((String)msgHeaders.get("nodeId")); 
        } 
        // else the priority is 1

        
        // while we are looking at the message headers, split off the resourcemaps
        QueueAccess destinationQueue;
        if (msgHeaders.containsKey("formatType")) {
            if (msgHeaders.get("formatType").equals("RESOURCE_MAP")) {
                destinationQueue = new QueueAccess(connFact, RESOURCE_MAP_QUEUE_NAME);
            } else {
                destinationQueue = new QueueAccess(connFact, READY_TO_PROCESS_QUEUE_NAME);
            }
        } else {
            destinationQueue = new QueueAccess(connFact, FAILED_TRIAGE_QUEUE_NAME);
        }
        
        message.getMessageProperties().setPriority((int)priority);
        destinationQueue.publish(message);
        return true;
                
    }
}
