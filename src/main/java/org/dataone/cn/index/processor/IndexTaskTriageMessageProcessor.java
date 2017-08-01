package org.dataone.cn.index.processor;

import java.util.Map;

import org.apache.log4j.Logger;
import org.dataone.cn.messaging.QueueAccess;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.stereotype.Component;

//@Component
public class IndexTaskTriageMessageProcessor {

     private static final Logger logger = Logger.getLogger(IndexTaskTriageMessageProcessor.class);
            
    private CachingConnectionFactory connFact = new CachingConnectionFactory("localhost");
    
    // TODO: centralize the queue names in own enumeration or map to the serverConfig
    public final static String RESOURCE_MAP_QUEUE_NAME = "indexing.waitingForReadinessTaskQueue";
    public final static String READY_TO_PROCESS_QUEUE_NAME = "indexing.prioritizedTaskQueue";

    public final static int PRIORITY_RANGE = 5;
    public final static int PRIORITY_LOOKBACK = 100;
    
    private QueuePrioritizer qp;
    
    public IndexTaskTriageMessageProcessor() {
        qp = new QueuePrioritizer(PRIORITY_LOOKBACK,PRIORITY_RANGE);
        connFact.setUsername("guest");
        connFact.setPassword("guest");
        connFact.setPublisherConfirms(true);
    }

    /**
     * this method assigns a relative priority by source nodeId
     * and also splits the input into two streams, one for resourceMaps, and the other for the rest 
     */
    @RabbitHandler
    public void processMessage(Message message) {
 
        
        Map<String,Object> msgHeaders = message.getMessageProperties().getHeaders();
        
        //determine the relative priority based on nodeId message header 
        float priority = 1.0f;
        if (msgHeaders.containsKey("nodeId")) {
            priority = qp.pushNext((String)msgHeaders.get("nodeId")); 
        } 
        // else the priority is 1

        logger.info(String.format("message received. nodeID = %s : formatType = %s : pid = %s : priority = %d",
                msgHeaders.get("nodeId"),
                msgHeaders.get("formatType"),
                msgHeaders.get("pid"),
                (int)priority
                ));
        
        // while we are looking at the message headers, split off the resourcemaps
        QueueAccess destinationQueue;
        if (msgHeaders.containsKey("formatType")) {
            if (msgHeaders.get("formatType").equals("RESOURCE")) {
                destinationQueue = new QueueAccess(connFact, RESOURCE_MAP_QUEUE_NAME);
            } else {
                destinationQueue = new QueueAccess(connFact, READY_TO_PROCESS_QUEUE_NAME);
            }
        } else {
            // fail it
            
            // destinationQueue = new QueueAccess(connFact, FAILED_TRIAGE_QUEUE_NAME);
            return;
        }
        
        message.getMessageProperties().setPriority((int)priority);
        logger.info("IndexTaskTriageMessageProcessor.processMessage - the index message for the object "+msgHeaders.get("pid")+" will be distributed from the new task queue to the queue"+destinationQueue.getQueueName());
        destinationQueue.publish(message);                
    }
}
