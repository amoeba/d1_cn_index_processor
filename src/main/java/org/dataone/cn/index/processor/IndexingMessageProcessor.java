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
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementAdd;
import org.dataone.cn.messaging.QueueAccess;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

@Component
//@RabbitListener(queues = "indexing.prioritizedTaskQueue")
public class IndexingMessageProcessor {


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
        System.out.println("entering index message processor#processMessage");
        try {
            IndexTask it = IndexTask.deserialize(message.getBody());

            InputStream sysmetaStream = new ByteArrayInputStream(it.getSysMetadata().getBytes());
            
            Map<String,SolrDoc> docMap = solrIndexService.parseTaskObject(it.getPid(), sysmetaStream, it.getObjectPath());
            Collection<SolrDoc> docs = docMap.values();
            SolrElementAdd addCommand = new SolrElementAdd(new ArrayList(docs));
          
            addCommand.serialize(System.out, "UTF-8");
            System.out.println();
            long stop = System.currentTimeMillis();
            System.out.println("processMessage WallTime: " + (stop-start));
            
            
            QueueAccess destinationQueue = new QueueAccess(connFact, "indexing.finalTaskQueue");
            destinationQueue.publish(message);
           
           
            
        } catch (ClassNotFoundException | IOException | XPathExpressionException |
                SAXException | ParserConfigurationException | EncoderException e) {
            
            throw new RuntimeException(e);
            
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }
        finally {}

    }
}
