package org.dataone.cn.index.processor;

import java.io.IOException;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;


/**
 * This is a processing class that prioritizes and sorts new IndexTasks from
 * a queue and puts them on two queues, one for New ResourceMapas and one
 * for all the rest.
 * 
 * There probably should be only one of these running, otherwise there might be
 * inaccurate prioritization = "striped" 
 */
public class TaskPrioritizerProcessor implements Runnable {

    public TaskPrioritizerProcessor() {
        // TODO Auto-generated constructor stub
    }

    
    @Override
    public void run() {
        // read from input queue
        final Channel ch = getInputQueue();
        final QueuePrioritizer qp = new QueuePrioritizer(100,5);
        try {
            boolean autoAck = false; 
            
            ch.basicConsume(getInputQueueName(), autoAck, "myConsumerTag", new DefaultConsumer(ch) 
            {

                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        AMQP.BasicProperties properties,
                        byte[] body)
                throws IOException 
                {
                    String routingKey = envelope.getRoutingKey();
                    String contentType = properties.getContentType();
                    long deliveryTag = envelope.getDeliveryTag();

                    // (process the message components here ...)
                    Map<String,Object> msgHeaders = properties.getHeaders();

                    float priority = 1.0f;
                    if (msgHeaders.containsKey("nodeId")) {
                        priority = qp.pushNext((String)msgHeaders.get("nodeId")); 
                    }

                    // XXX: should this be handled by an exchange instead?
                    // the problem with exchanges are that they can cause 
                    // tasks to be dropped if there are no subscribers to the 
                    // message topic.
                    //
                    // Durable (and bound) Topic queues might solve this problem?
                    // see 'mandatory' flag upon publish in https://www.rabbitmq.com/reliability.html
                    
                    // Advantages of using pub-sub: routing could live in config, rather than code.
                    // in this case, tho', it seems that the queues are closely tied to the prioritization
                    // logic - we know there is structurally a big difference between ResMap and Metadata tasks
                    // further splits might be handled by pub-sub, but this first split seems fundamental
                    if (msgHeaders.containsKey("formatType")) {
                        if (msgHeaders.get("formatType").equals("RESOURCE_MAP")) {
                            
                            Channel outChannel = getResourceMapQueue();
                            outChannel.basicPublish("", "RESOURCE_MAP_QUEUE",
                                    MessageProperties.PERSISTENT_BASIC,
                                    body);
                            try {
                                ch.waitForConfirmsOrDie();
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            // send to NewResourceMapToProcessQueue
                            // don't forget to set the priority
                            // don't forget to set up publisher confirmation
                        } else {
                            // send to ReadyToProcessQueue
                            // don't forget to set the priority
                            // don't forget to set up publisher confirmation
                        }
                        
                        
                    }
                    ch.basicAck(deliveryTag, false);
                }
            }  // the anonymous DefaultConsumer

                    );  //basicConsume
            
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            // need to 
            e.printStackTrace();
        } finally {}
    }
    
    /*
     * The Channel from whence to read...
     */
    public Channel getInputQueue() {
        return null;
    }
    
    /* 
     * The basicConsume needs the queue name of the channel we are reading from
     */
    public String getInputQueueName() {
        return null;
    }
    
    public Channel getResourceMapQueue() {
        return null;
    }

    public Channel getReadyToProcessQueue() {
        return null;
    }
}
