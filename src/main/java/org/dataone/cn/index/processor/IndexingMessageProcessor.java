package org.dataone.cn.index.processor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.messaging.QueueAccess;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

//@Component
public class IndexingMessageProcessor {

    private static final Logger logger = Logger.getLogger(IndexingMessageProcessor.class);
    @Autowired
    SolrIndexService solrIndexService;

    private CachingConnectionFactory connFact = new CachingConnectionFactory("localhost");
        
    public IndexingMessageProcessor() {
        connFact.setUsername("guest");
        connFact.setPassword("guest");
        connFact.setPublisherConfirms(true);
    }

    public IndexingMessageProcessor(SolrIndexService service) {
        solrIndexService = service;
    }


    @RabbitHandler
    public void processMessage(Message message) {
        long start = System.currentTimeMillis();
        logger.info("IndexingMessageProcessor.processMessage - entering index message processor#processMessage");
        String pid = "unknown";
        try {
            IndexTask it = IndexTask.deserialize(message.getBody());
            logger.info("IndexingMessageProcessor.processMessage - after transform the message body to the IndextTask object for the object "+it.getPid());
            InputStream sysmetaStream = new ByteArrayInputStream(it.getSysMetadata().getBytes());
            
            // So far, this processor only parses the fields in the systemMetadata and metadata
            // contained in the indexTask itself.  (It is not calling out to Solr to compare as
            // we would do in production.
            
            /*Map<String,SolrDoc> docMap = solrIndexService.parseTaskObject(it.getPid(), sysmetaStream, it.getObjectPath());
            Collection<SolrDoc> docs = docMap.values();
            SolrElementAdd addCommand = new SolrElementAdd(new ArrayList(docs));
          
            // prints the solr add command to Stdout
            addCommand.serialize(System.out, "UTF-8");
            System.out.println();*/
            
                   
            // this is likely the production call instead of the above 5 lines
            // but does processObject handle delete tasks as well?
            
            //SolrElementAdd addCommand = solrIndexService.processObject(it.getPid(), sysmetaStream, it.getObjectPath());
            solrIndexService.insertIntoIndex(it.getPid(), sysmetaStream, it.getObjectPath());            
            long stop = System.currentTimeMillis();
            logger.info("IndexingMessageProcessor.procesMessage - successfully indexed the object "+ it.getPid()+" and the processing time takes : " + (stop-start)+ " milliseconds.");
            
            // if success, we probably won't push the task to a queue (because it will never be read 
            // and the queue will just fill up, but it's helpful for now while we're testing

            QueueAccess destinationQueue = new QueueAccess(connFact, "indexing.finalTaskQueue");
            destinationQueue.publish(message);
           
           
            
        } catch (ClassNotFoundException | IOException | XPathExpressionException |
                SAXException | ParserConfigurationException | EncoderException e) {
            logger.error("Failed to index "+pid, e);
            throw new RuntimeException(e);
            
        } catch (NullPointerException npe) {
            logger.error("Failed to index "+pid, npe);
            npe.printStackTrace();
        } catch(Exception e) {
            logger.error("Failed to index "+pid, e);
        }
        finally {}

    }
}
