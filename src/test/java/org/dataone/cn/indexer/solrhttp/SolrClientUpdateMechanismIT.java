package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.dataone.cn.indexer.parser.UpdateAssembler;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.xml.sax.SAXException;
/**
 * the tests in this class can be used to test the behavior of
 * different solrClients with regards to concurrent updates.
 * 
 * (You will need to comment the @Ignore annotations to run the tests,
 * they are ignored because the tests require either an independent
 * local solr service, or port-forwarding to a live instance
 * 
 *  for example: ssh -L8983:localhost:8983 cn-dev-orc-1.test.dataone.org
 * 
 * @author rnahf
 *
 */
public class SolrClientUpdateMechanismIT {

    String baseurl = "http://localhost:8983/solr";
    String collection_core = "search_core";
    
    static SolrSchema schema;
    static String schemaPath = "./src/test/resources/org/dataone/cn/index/resources/solr5home/collection1/conf/schema.xml";
    
    @BeforeClass
    static public void beforeClass() {
        ApplicationContext context = new ClassPathXmlApplicationContext("/org/dataone/cn/indexer/solrhttp/test-solr-schema.xml");
        schema = (SolrSchema) context.getBean("solrSchema");       
    }
    
    SolrClient httpSolrClient;
    SolrClient concurrentUpdateClient;
    SolrClient customConMgrHttpSolrClient;
    @Before
    public void setUp() {
        
        // A
        httpSolrClient = new HttpSolrClient(baseurl);
        
        // B
       concurrentUpdateClient = new ConcurrentUpdateSolrClient(baseurl,10,1);
        
        // C
        PoolingHttpClientConnectionManager conman = new PoolingHttpClientConnectionManager();
        conman.setMaxTotal(1);
        HttpClientBuilder httpClientBuilder = HttpClients.custom();
        httpClientBuilder.setConnectionManager(conman);
        HttpClient httpClient = httpClientBuilder.build();
//        HttpClient httpClient = HttpClients.createMinimal(conman);
        customConMgrHttpSolrClient = new HttpSolrClient(baseurl+"/"+collection_core, httpClient);
        
        // D
//      SolrClient client = new CloudSolrClient("localhost:2181");
    }
  
    private Object buildValue(String modifier,Object value) {
        if (modifier == null) 
            return value;
        
        Map<String, Object> fieldModifier = new HashMap<>(1);
        fieldModifier.put(modifier, value);
        return fieldModifier;
    
    }
    
    private boolean isConflict(Exception e, boolean testStep, String clientName) {
        // SolrServerException / java.net.ConnectException - when portforwarding not active
        // HttpSolrClient.RemoteSolrException (is-a SolrException)/   
        
        System.out.println("at testing step : " + testStep);
        System.out.println("for Client class: " + clientName);
        System.out.println("Exception thrown: " + e.getClass().getCanonicalName());
        System.out.println("Exception message: " + e.getMessage());
        if (e instanceof NullPointerException) {
            e.printStackTrace();
        }
        if (e.getCause() != null) {
            System.out.println("Exception cause: " + e.getCause().getClass().getCanonicalName());
            System.out.println("Cause message: " + e.getMessage());
        }
        
        if (testStep) {       
            if (e instanceof  HttpSolrClient.RemoteSolrException) {          
                return true;
            } else {
                return false;
              //  fail ("Wrong kind of exception thrown?");
            }
        } else {
            fail ("Exception thrown at the wrong step");
            return false;
        }
    }
    
    @Ignore("can only be run with portforwarding to a CN")
    @Test
    public void testUpdate_OptimisticConcurrency_BasicClientExceptions() throws SolrServerException, IOException, InterruptedException {
        
        SolrClient client = customConMgrHttpSolrClient;
        
        int step = 0;
        try {
            
            client.deleteById("book1234");
            SolrDocument shouldBeEmpty = client.getById("book1234");
            assertNull(shouldBeEmpty);
            
            final SolrInputDocument sid = new SolrInputDocument();
            sid.addField("id", buildValue(null,"book1234"));
            sid.addField("title", buildValue(null,"all the pretty horses"));
            sid.addField("abstract", "it's a fiction book");
            sid.addField("_version_", "-1");

            final SolrInputDocument sid2 = new SolrInputDocument();
            sid2.addField("id", buildValue(null,"book1234"));
            sid2.addField("title", buildValue(null,"on the road"));
            sid2.addField("_version_", "-1");

            //        SolrInputDocument sid3 = new SolrInputDocument();
            //        sid3.addField("id", buildValue(null,"book1234"));
            //        sid3.addField("title", buildValue(null,"no country for old men"));
            //        sid3.addField("_version_", "-1");

            
 
            step = 1;
            client.add(sid, 0);
            step = 2;
            client.add(sid2,0);
            
            //        step = 3;
            //        client.add(sid3,1000);
            fail("Should have gotten a conflict");
        
        // SolrServerException / java.net.ConnectException - when portforwarding not active
        // HttpSolrClient.RemoteSolrException (is-a SolrException)/     
        }
        catch (Exception e) {
            
            isConflict(e,
                    (step == 2) ? true: false,
                            client.getClass().getCanonicalName());
         
            
        } finally {
            Thread.sleep(1500);
            
            client.close();
        }
        
    }
    
    
    @Ignore("can only be run with portforwarding to a CN")
    @Test
    public void testUpdate_OptimisticConcurrency_MultiThreaded() throws SolrServerException, IOException, InterruptedException {
               
        final AtomicInteger conflictsEncountered = new AtomicInteger();
        final SolrClient client = customConMgrHttpSolrClient;
        
//       SolrClient client = new MyConcurrentUpdateSolrClient(baseurl,10,1);
//        SolrClient client = new CloudSolrClient("localhost:2181");
        
        ExecutorService es = Executors.newFixedThreadPool(2);
        
        int step = 0;
        try {
            
            client.deleteById("book1234");
            SolrDocument shouldBeEmpty = client.getById("book1234");
            assertNull(shouldBeEmpty);
            
            final SolrInputDocument sid = new SolrInputDocument();
            sid.addField("id", buildValue(null,"book1234"));
            sid.addField("title", buildValue(null,"all the pretty horses"));
            sid.addField("abstract", "it's a fiction book");
            sid.addField("_version_", "-1");

            final SolrInputDocument sid2 = new SolrInputDocument();
            sid2.addField("id", buildValue(null,"book1234"));
            sid2.addField("title", buildValue(null,"on the road"));
            sid2.addField("_version_", "-1");

            //        SolrInputDocument sid3 = new SolrInputDocument();
            //        sid3.addField("id", buildValue(null,"book1234"));
            //        sid3.addField("title", buildValue(null,"no country for old men"));
            //        sid3.addField("_version_", "-1");

            
            es.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                client.add(sid,200);
                            } catch (Exception e) {
                                if (isConflict(e,true,client.getClass().getCanonicalName())) {
                                    conflictsEncountered.addAndGet(1); 
                                }
                            }
                        }
                    });
            
            es.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                client.add(sid2,200);
                            } catch (Exception e) {
                                if (isConflict(e,true,client.getClass().getCanonicalName())) {
                                    conflictsEncountered.addAndGet(1); 
                                }
                                        
                                        ;
                            }
                        }
                    });

            
                                     
        } finally {
 //           Thread.sleep(2500); 
            es.awaitTermination(2500, TimeUnit.MILLISECONDS);
            assertTrue("Should have gotten a conflict",conflictsEncountered.get() > 0);
            client.close();
        }       
    }
    
    @Ignore("can only be run with portforwarding to a CN")
    @Test
    public void testUpdate_OptimisticConcurrency_MultiThreaded_Retry() throws SolrServerException, IOException, InterruptedException, ParserConfigurationException, SAXException {
          
       
        final AtomicInteger conflictsEncountered = new AtomicInteger();
        final SolrClient updateClient = customConMgrHttpSolrClient;
        final SolrJClient d1solrC = new SolrJClient(schema, updateClient, new HttpSolrClient(baseurl+"/"+collection_core));
        d1solrC.setSolrSchemaPath(schemaPath);
        d1solrC.setSolrIndexUri(baseurl+"/"+collection_core);
        
//       SolrClient client = new MyConcurrentUpdateSolrClient(baseurl,10,1);
//        SolrClient client = new CloudSolrClient("localhost:2181");
        
        
        
        ExecutorService es = Executors.newFixedThreadPool(2);
        
        int step = 0;
        try {
            
            try {
                updateClient.deleteById("book1234");
                SolrDocument shouldBeEmpty = updateClient.getById("book1234");
                assertNull(shouldBeEmpty);
            } catch(Exception e) {
                if (isConflict(e,false,updateClient.getClass().getCanonicalName())) {
                    conflictsEncountered.addAndGet(1); 
                }
            }
            
            
            final UpdateAssembler ua1 = new UpdateAssembler(schema);
            final SolrDocument sid = new SolrDocument();
            sid.addField("id", buildValue(null,"book1234"));
            sid.addField("title", buildValue(null,"all the pretty horses"));
            sid.addField("abstract", "it's a fiction book");
            sid.addField("_version_", "-1");
            ua1.addToUpdate("book1234", null, convert(sid));

            final UpdateAssembler ua2 = new UpdateAssembler(schema);
            final SolrDocument sid2 = new SolrDocument();
            sid2.addField("id", buildValue(null,"book1234"));
            sid2.addField("documents", buildValue(null,"a possible future"));
            sid2.addField("_version_", "-1");
            ua2.addToUpdate("book1234", null, convert(sid2));
            
            //        SolrInputDocument sid3 = new SolrInputDocument();
            //        sid3.addField("id", buildValue(null,"book1234"));
            //        sid3.addField("title", buildValue(null,"no country for old men"));
            //        sid3.addField("_version_", "-1");
            
//////    uncomment this block to try in the main thread            
//            boolean flag = false;
//            try {
//                d1solrC.sendUpdate(baseurl+"/"+collection_core, ua1, false);
////                              client.add("search_core",sid,200);
//                flag = true;
//                d1solrC.sendUpdate(baseurl+"/"+collection_core, ua2, false);
//            } catch (Exception e) {
//                if (isConflict(e,flag,d1solrC.getClass().getCanonicalName())) {
//                    conflictsEncountered.addAndGet(1); 
//                }
//            }
            
            es.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                d1solrC.sendUpdate(baseurl+"/"+collection_core, ua1, false);
  //                              client.add("search_core",sid,200);
                            } catch (Exception e) {
                                if (isConflict(e,false,d1solrC.getClass().getCanonicalName())) {
                                    conflictsEncountered.addAndGet(1); 
                                }
                            }
                        }
                    });
            
            es.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                d1solrC.sendUpdate(baseurl+"/"+collection_core, ua2, false);
                       //         client.add("search_core",sid2,200);
                            } catch (Exception e) {
                                if (isConflict(e,true,d1solrC.getClass().getCanonicalName())) {
                                    conflictsEncountered.addAndGet(1); 
                                }
                            }
                        }
                    });
        
            
                                     
        } finally { 
            es.awaitTermination(2500, TimeUnit.MILLISECONDS);
           
            SolrDocument doc = updateClient.getById("book1234");
            assertEquals("all the pretty horses", doc.getFieldValue("title"));
            assertEquals("a possible future", ((List<String>)doc.getFieldValue("documents")).get(0));
        }       
    }
    
    // only works with string values
    private SolrDoc convert(SolrDocument sd) {
        SolrDoc doc = new SolrDoc();
        for (Map.Entry<String,Object> field :sd.entrySet()) {
            SolrElementField sef = new SolrElementField();
            sef.setName(field.getKey());
            //            if (field.getValue() instanceof Map) {
            //                if ( sef.setModifier( ((Map.Entry)field.getValue()).getKey());)
            //                
            //                
            //                sef.setValue( (String)((Map.Entry)field.getValue()).getValue());
            //            } else {
            sef.setValue((String)field.getValue());   
            doc.addField(sef);
        }
        return doc;
    
    }
    
}
