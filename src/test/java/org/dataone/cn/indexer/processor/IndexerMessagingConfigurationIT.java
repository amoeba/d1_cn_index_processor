package org.dataone.cn.indexer.processor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.dataone.cn.index.messaging.MessagingClientConfiguration;
import org.dataone.cn.index.messaging.MessagingServerConfiguration;
import org.dataone.cn.index.processor.IndexTaskTriageMessageProcessor;
import org.dataone.cn.index.processor.IndexingMessageProcessor;
import org.dataone.cn.messaging.QueueAccess;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;


public class IndexerMessagingConfigurationIT {
    
    @Test
    public void indexServerConfigurationIT() {
        ApplicationContext serverContext = new AnnotationConfigApplicationContext(MessagingServerConfiguration.class);
        Queue newTaskQueue = (Queue) serverContext.getBean("newTaskQueue");
        System.out.println("queueName: " + newTaskQueue.getName());
    }
    
    
    
    @Test
    public void indexConsumerConfigurationIT() throws InterruptedException {
        ApplicationContext clientContext = new AnnotationConfigApplicationContext(MessagingClientConfiguration.class);
        
        CachingConnectionFactory cf = (CachingConnectionFactory) clientContext.getBean("rabbitConnectionFactory");
        clientContext.getBean("messageListenerContainer");
        QueueAccess newTaskQueue = new QueueAccess(cf, "indexing.newTaskQueue");
        
        MessageListener ml = (MessageListener) clientContext.getBean("exampleListener");
        newTaskQueue.registerAsynchronousMessageListener(1, ml); 
        
        
        Message m = MessageBuilder.withBody("A MESSAGE!!!".getBytes())
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .build();
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        
        

        Thread.sleep(10000);
        
    }

    @Test
    public void indexConsumerConfiguration_PojoListener_IT() throws InterruptedException {
        
        ApplicationContext clientContext = new AnnotationConfigApplicationContext(MessagingClientConfiguration.class);
        

        CachingConnectionFactory cf = (CachingConnectionFactory) clientContext.getBean("rabbitConnectionFactory");
        clientContext.getBean("messageListenerAdapterContainer");
        QueueAccess newTaskQueue = new QueueAccess(cf, "indexing.newTaskQueue");
        
//        MessageListener ml = (MessageListener) clientContext.getBean("exampleListenerAdapter");
//        newTaskQueue.registerAsynchronousMessageListener(1, ml); 
        
        
        Message m = MessageBuilder.withBody("A MESSAGE!!!".getBytes())
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .build();
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        newTaskQueue.publish(m);
        
        Thread.sleep(5000);
    }
    
    
    
    
    
}
