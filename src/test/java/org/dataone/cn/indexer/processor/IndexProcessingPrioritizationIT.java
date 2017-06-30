package org.dataone.cn.indexer.processor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.dataone.client.exception.ClientSideException;
import org.dataone.client.v2.CNode;
import org.dataone.client.v2.itk.D1Client;
import org.dataone.cn.index.messaging.IndexProcessingPipelineManager;
import org.dataone.cn.index.messaging.MessagingClientConfiguration;
import org.dataone.cn.index.messaging.TestMessagingClientConfiguration;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.ResourceMapIndexTask;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.messaging.QueueAccess;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.InvalidRequest;
import org.dataone.service.exceptions.InvalidToken;
import org.dataone.service.exceptions.NotAuthorized;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.exceptions.NotImplemented;
import org.dataone.service.exceptions.ServiceFailure;
import org.dataone.service.types.v1.ObjectInfo;
import org.dataone.service.types.v1.ObjectList;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.ObjectFormatList;
import org.dataone.service.types.v2.SystemMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;


public class IndexProcessingPrioritizationIT {


    
//    public static final Logger logger = Logger.getLogger(IndexProcessingPrioritizationIT.class);
 
    private static HazelcastInstance hzMember;
    private HazelcastInstance hzClient;

//    String synchronizationObjectQueue =
//            Settings.getConfiguration().getString("dataone.hazelcast.synchronizationObjectQueue");
    String hzNodesName =
            Settings.getConfiguration().getString("dataone.hazelcast.nodes");
    String hzSystemMetaMapString =
            Settings.getConfiguration().getString("dataone.hazelcast.systemMetadata");

    
    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUpContext() throws ClientSideException, NotImplemented, ServiceFailure {

        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }

        System.out.println();
        hzMember = Hazelcast.newHazelcastInstance(hzConfig);
//        hzMember = HazelcastClientFactory.getStorageClient();
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

    }
    
    
    
    
    
    
    /**
     * this test creates a bunch of IndexTasks from listObjects / getSystemMetadata
     * 
     * @throws InterruptedException
     * @throws ServiceFailure
     * @throws InvalidRequest
     * @throws InvalidToken
     * @throws NotAuthorized
     * @throws NotImplemented
     * @throws IOException
     * @throws ClientSideException
     */
    @SuppressWarnings("resource")
//    @Test
    public void indexConsumerConfiguration_PojoListener_IT() throws InterruptedException, ServiceFailure, InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, IOException, ClientSideException {

        ApplicationContext messagingContext = new AnnotationConfigApplicationContext(TestMessagingClientConfiguration.class);
//        ApplicationContext mainContext = new ClassPathXmlApplicationContext("test-context.xml");

        CachingConnectionFactory cf = (CachingConnectionFactory) messagingContext.getBean("rabbitConnectionFactory");
        messagingContext.getBean("messageListenerAdapterContainer");
        messagingContext.getBean("messageListenerAdapterContainer2");
        QueueAccess newTaskQueue = new QueueAccess(cf, "indexing.newTaskQueue");

        //        MessageListener ml = (MessageListener) clientContext.getBean("exampleListenerAdapter");
        //        newTaskQueue.registerAsynchronousMessageListener(1, ml); 



        CNode cn = D1Client.getCN("https://cn-stage-orc-1.test.dataone.org/cn");
        ObjectList ol = cn.listObjects(null,null, null, null, null, null, 0, 1000);

        // map format names to format types
        Map<String,String>  formatTypes = new HashMap<>();
        ObjectFormatList ofl = cn.listFormats();
        for (ObjectFormat of : ofl.getObjectFormatList()) {
            formatTypes.put(of.getFormatName(), of.getFormatType());
        }
        System.out.println("# registered formats: " + formatTypes.size());


        int count = 0;
        for(ObjectInfo oi  : ol.getObjectInfoList()) {
            try {
                long t0 = System.currentTimeMillis();

                SystemMetadata sm = cn.getSystemMetadata(null, oi.getIdentifier());

                long t1 = System.currentTimeMillis();

                InputStream is = null;
                FileOutputStream fos = null;
                File tmp = null;
                try {
                    tmp = File.createTempFile("indexEchoTest.", ".xml");
                    is = cn.get(null, sm.getIdentifier());
                    fos = new FileOutputStream(tmp);
                    IOUtils.copy(is, fos);
                } catch (NotFound | NotAuthorized e) {
                    tmp = null;
                } catch (Exception e) {
                    e.printStackTrace();
                    tmp=null;
                } finally {
                    IOUtils.closeQuietly(is);
                    IOUtils.closeQuietly(fos);
                }
                String objectPath = tmp != null ? tmp.getAbsolutePath() : null;
                IndexTask it = new IndexTask(sm,objectPath);

                long t2 = System.currentTimeMillis();

                String formatType = formatTypes.get(oi.getFormatId().getValue());
                if (formatType == null) {
                    formatType = oi.getFormatId().getValue();
                }
                Message m = MessageBuilder.withBody(it.serialize())
                        .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                        .setHeader("nodeId", sm.getAuthoritativeMemberNode().getValue())
                        .setHeader("formatType", formatType)
                        .setHeader("pid", oi.getIdentifier().getValue())
                        .build();

                long t3 = System.currentTimeMillis();

                newTaskQueue.publish(m);


                long t4 = System.currentTimeMillis();
                count++;

                System.out.println(String.format("timings [%d]: %d, %d, %d %d", count, t1-t0, t2-t1, t3-t2, t4-t3));
                

            } catch (Exception e) {
                e.printStackTrace();
//                System.out.println(e.getMessage());
            }
        }

        Thread.sleep(60000);

    }
    
//    public void main(String[] args) {
//        try {
//            testConsumerContainerSetup();
//        } catch (ServiceFailure | InvalidRequest | InvalidToken | NotAuthorized
//                | NotImplemented | InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//    }
    
    @Test
    public void testQueueProperties() {
        Properties props = Settings.getConfiguration().getProperties("dataone.index.queue");
        for (Object key: props.keySet()) {
            System.out.println(key + ": " + props.getProperty((String) key));
            
        }
    }
    
    
    @Test
    public void consumerContainerSetup_IT() throws ServiceFailure, InvalidRequest, InvalidToken, NotAuthorized, NotImplemented, InterruptedException {
  
        
        ApplicationContext clientContext = new AnnotationConfigApplicationContext(TestMessagingClientConfiguration.class);
        CachingConnectionFactory cf = (CachingConnectionFactory) clientContext.getBean("rabbitConnectionFactory");
 
        QueueAccess newTaskQueue = (QueueAccess) clientContext.getBean("newTaskQueueAccess");
        
            // prepare a list of tasks

            List<Message> messageList = new ArrayList<>();

            CNode cn = D1Client.getCN("https://cn-stage-orc-1.test.dataone.org/cn");
            ObjectList ol = cn.listObjects(null,null, null, null, null, null, 22, 4);

            // map format names to format types
            Map<String,String>  formatTypes = new HashMap<>();
            ObjectFormatList ofl = cn.listFormats();
            for (ObjectFormat of : ofl.getObjectFormatList()) {
                formatTypes.put(of.getFormatId().getValue(), of.getFormatType());
            }
            System.out.println("# registered formats: " + formatTypes.size());



            int count = 0;
            for(ObjectInfo oi  : ol.getObjectInfoList()) {
                try {
                    long t0 = System.currentTimeMillis();

                    SystemMetadata sm = cn.getSystemMetadata(null, oi.getIdentifier());

                    long t1 = System.currentTimeMillis();

                    InputStream is = null;
                    FileOutputStream fos = null;
                    File tmp = null;
                    try {
                        tmp = File.createTempFile("indexEchoTest.", ".xml");
                        is = cn.get(null, sm.getIdentifier());
                        fos = new FileOutputStream(tmp);
                        IOUtils.copy(is, fos);
                    } catch (NotFound | NotAuthorized e) {
                        tmp = null;
                    } catch (Exception e) {
                        e.printStackTrace();
                        tmp=null;
                    } finally {
                        IOUtils.closeQuietly(is);
                        IOUtils.closeQuietly(fos);
                    }
                    String objectPath = tmp != null ? tmp.getAbsolutePath() : null;
                    IndexTask it = new IndexTask(sm,objectPath);

                    long t2 = System.currentTimeMillis();

                    
                    String formatType = formatTypes.get(oi.getFormatId().getValue());
                    if (formatType == null) {
                        formatType = oi.getFormatId().getValue();
                    }
                     
                    byte[] serializedTask;
                    if (formatType.equals("RESOURCE")) {
                        ResourceMapIndexTask rmit = new ResourceMapIndexTask();
                        rmit.copy(it);
                        serializedTask = rmit.serialize(); 
                    } else {
                        serializedTask = it.serialize();
                    }
                    
                    Message m = MessageBuilder.withBody(serializedTask)
                            .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                            .setHeader("nodeId", sm.getAuthoritativeMemberNode().getValue())
                            .setHeader("formatType", formatType)
                            .setHeader("pid", oi.getIdentifier().getValue())
                            .build();

                    long t3 = System.currentTimeMillis();

                    messageList.add(m);

                    System.out.println("Prepared task for id: " + oi.getIdentifier().getValue() + " [" + formatType + "]");

                    long t4 = System.currentTimeMillis();
                    count++;

                    //                logger.warn(String.format("timings [%d]: %d, %d, %d %d", count, t1-t0, t2-t1, t3-t2, t4-t3));


                } catch (Exception e) {
                    e.printStackTrace();
                    //                System.out.println(e.getMessage());
                }

            }
            
            System.out.println(".........Submitting Tasks to newTaskQueue.......");
            

            for (Message m : messageList) {
                System.out.println("%%%%%%%%%%% publish start: " + new Date());
                newTaskQueue.publish(m);
                System.out.println("########### publish finish: " + new Date());
            }      
        
            new IndexProcessingPipelineManager(clientContext);
            


            System.out.println(".........Waiting for Tasks to Process.......");


            Thread.sleep(10000);
            System.out.println(".........10 seconds passed.......");


            Thread.sleep(10000);
            System.out.println(".........20 seconds passed.......");


            Thread.sleep(20000);
            System.out.println(".........40 seconds passed.......");


            Thread.sleep(10000);
  //      }
    }
}
