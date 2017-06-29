package org.dataone.cn.index.messaging;

import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;


@Configuration
public class MessagingServerConfiguration {

    @Bean
    public CachingConnectionFactory rabbitConnectionFactory() {
        CachingConnectionFactory connectionFactory =
                new CachingConnectionFactory("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        connectionFactory.setPublisherConfirms(true);
        connectionFactory.setPublisherReturns(false);
        
        return connectionFactory;
    }
    
    @Bean
    public RabbitAdmin rabbitAdmin() {
        return new RabbitAdmin(rabbitConnectionFactory());
    }

    @Bean
    public Queue newTaskQueue() {
        Queue queue = new Queue("indexing.newTaskQueue");
        rabbitAdmin().declareQueue(queue);
        return queue;
    }
    
    @Bean
    public Queue prioritizedTaskQueue() {
        Queue queue = new Queue("indexing.prioritizedTaskQueue");
        rabbitAdmin().declareQueue(queue);
        return queue;
    }
    
    @Bean
    public Queue waitingForReadinessTaskQueue() {
        Queue queue = new Queue("indexing.waitingForReadinessTaskQueue");
        rabbitAdmin().declareQueue(queue);
        return queue;
    }
    
    @Bean
    public Queue finalTaskQueue() {
        Queue queue = new Queue("indexing.finalTaskQueue");
        rabbitAdmin().declareQueue(queue);
        return queue;
    }
    
    @Bean
    public Queue delayedRetryQueue5s() {
        Queue q = QueueBuilder.durable("indexing.delayedRetryQueue5s")
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", "indexing.waitingForReadinessTaskQueue")
                .withArgument("x-message-ttl", 5000) //ms
                .build();
        rabbitAdmin().declareQueue(q);
        return q;
    }
    
    @Bean
    public Queue delayedRetryQueue10s() {
        Queue q = QueueBuilder.durable("indexing.delayedRetryQueue10s")
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", "indexing.waitingForReadinessTaskQueue")
                .withArgument("x-message-ttl", 10000) //ms
                .build();
        rabbitAdmin().declareQueue(q);
        return q;
    }
    
    @Bean
    public Queue delayedRetryQueue20s() {
        Queue q = QueueBuilder.durable("indexing.delayedRetryQueue20s")
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", "indexing.waitingForReadinessTaskQueue")
                .withArgument("x-message-ttl", 20000) //ms
                .build();
        rabbitAdmin().declareQueue(q);
        return q;
    }
    
    @Bean
    public Queue delayedRetryQueue2m() {
        Queue q = QueueBuilder.durable("indexing.delayedRetryQueue120s")
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", "indexing.waitingForReadinessTaskQueue")
                .withArgument("x-message-ttl", 120000) //ms
                .build();
        rabbitAdmin().declareQueue(q);
        return q;
    }
    
    @Bean
    public Queue delayedRetryQueue30m() {
        Queue q = QueueBuilder.durable("indexing.delayedRetryQueue1800s")
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", "indexing.waitingForReadinessTaskQueue")
                .withArgument("x-message-ttl", 1800000) //ms
                .build();
        rabbitAdmin().declareQueue(q);
        return q;
    }
}
