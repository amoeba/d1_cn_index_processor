package org.dataone.cn.index.messaging;

import org.dataone.cn.index.processor.IndexTaskTriageMessageProcessor;
import org.dataone.cn.index.processor.IndexingMessageProcessor;
import org.dataone.cn.index.processor.ResourceMapReadinessMessageProcessor;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.processor.MockD1IndexerSolrClient;
import org.dataone.cn.messaging.QueueAccess;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.ImportResource;
import org.springframework.context.annotation.Profile;

@Configuration
//@Profile("test")
@Import(MessagingServerConfiguration.class)
@ImportResource({"classpath:org/dataone/cn/index/test-context.xml"})
public class TestMessagingClientConfiguration {
    
    @Autowired
    CachingConnectionFactory rabbitConnectionFactory;
    
    @Autowired
    SolrIndexService solrIndexService;
    
    @Autowired
    D1IndexerSolrClient d1IndexerSolrClient;
    
    @Autowired
    String solrQueryUri;
    
    @Autowired @Qualifier("newTaskQueue")
    Queue newTaskQueue;
    
    @Autowired @Qualifier("prioritizedTaskQueue")
    Queue prioritizedTaskQueue;
    
    @Autowired @Qualifier("waitingForReadinessTaskQueue")
    Queue waitingForReadinessTaskQueue;
    
    @Autowired @Qualifier("finalTaskQueue")
    Queue finalTaskQueue;
    

   
     
       
    
    @Bean  @Qualifier("newTaskQueueAccess")
    public QueueAccess newTaskQueueAccess() {
        return new QueueAccess(rabbitConnectionFactory, newTaskQueue.getName());
    }
    
    @Bean @Qualifier("newTaskQueueListener")
    public MessageListener newTaskQueueListener() {
        MessageListenerAdapter ml = new MessageListenerAdapter(new IndexTaskTriageMessageProcessor());
        ml.setDefaultListenerMethod("processMessage");       
        ml.setMessageConverter(null); // we're happy to take a Message
        return ml;
    }
    
    
    @Bean @Qualifier("prioritizedTaskQueueAccess")
    public QueueAccess prioritizedTaskQueueAccess() {
        return new QueueAccess(rabbitConnectionFactory, prioritizedTaskQueue.getName());
    }
    
    @Bean @Qualifier("prioritizedTaskQueueListener")
    public MessageListener prioritizedTaskQueueListener() {
        MessageListenerAdapter ml = new MessageListenerAdapter(new IndexingMessageProcessor(solrIndexService));
        ml.setDefaultListenerMethod("processMessage");
        ml.setMessageConverter(null); // we're happy to take a Message
        return ml;
            
    }
    
    
    @Bean @Qualifier("waitingForReadinessTaskQueueAccess")
    public QueueAccess waitingForReadinessTaskQueueAccess() {
        return new QueueAccess(rabbitConnectionFactory, waitingForReadinessTaskQueue.getName());
    }
    
    @Bean @Qualifier("waitingForReadinessTaskQueueListener")
    public MessageListener waitingForReadinessTaskQueueListener() {
        
        
        // We are plugging in a NockD1IndexerSolrClient to avoid actualy communication with a Solr server
        MessageListenerAdapter ml = new MessageListenerAdapter(new ResourceMapReadinessMessageProcessor(new MockD1IndexerSolrClient(), solrQueryUri));
        ml.setDefaultListenerMethod("processMessage");
        ml.setMessageConverter(null); // we're happy to take a Message
        return ml;
            
    } 

}
