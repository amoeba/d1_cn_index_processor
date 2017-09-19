package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.configuration.Settings;
import org.dataone.exceptions.MarshallingException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.xml.sax.SAXException;

public class SolrJClientIT {

    @Autowired
    SolrIndexService indexService;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testTrue() {
        assertTrue("Should be true", true);
    }
   
    @Ignore ("this is an integration test")
    @Test
    public void parseResponse() throws XPathExpressionException, IOException, EncoderException, SolrServerException {
        
        // test requires port-forwarding unless you have a populated solr instance running locally 
        // of course, you will need ssh access to the host to forward to
        // for example: ssh -L 8983:localhost:8983 cn-stage-ucsb-1.test.dataone
        String solrCoreName = "search_core";
        SolrJClient client = new SolrJClient("http://localhost:8983/solr/" + solrCoreName);
        client.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
        
        
        SolrClient bareClient = new HttpSolrClient("http://localhost:8983/solr/"  + solrCoreName);
        // this is a known object in cn-stage
        QueryResponse qr = bareClient.query(new SolrQuery("id:14651b180c80823137427714ec2168035c953f61"));
        
        try {
            List<SolrDoc> results = client.parseResponse(qr.getResults());      
        
            for (SolrDoc sd : results) {
                sd.serialize(System.out, "UTF-8");
                System.out.println();
            }
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }
        
    }

    @Ignore ("this is an integration test")
    @Test
    public void getQueryById() throws XPathExpressionException, IOException, EncoderException, SolrServerException {
        
        // test requires port-forwarding unless you have a populated solr instance running locally 
        // of course, you will need ssh access to the host to forward to
        // for example: ssh -L 8983:localhost:8983 cn-stage-ucsb-1.test.dataone
        String solrCoreName = "search_core";
        SolrJClient client = new SolrJClient("http://localhost:8983/solr/" + solrCoreName);
        client.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
        
        try {
            List<SolrDoc> results = client.getDocumentBySolrId("", "tao.2013022113480472890");  
            assertTrue("Should get a result to parse", results.size() > 0);
            for (SolrDoc sd : results) {
                sd.serialize(System.out, "UTF-8");
                System.out.println();
            }
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }
        
    }
    
   @Ignore ("this is an integration test")
    @Test
    public void update() throws IOException { //XPathExpressionException, IOException, EncoderException, SolrServerException {
        
        // test requires port-forwarding unless you have a populated solr instance running locally 
        // of course, you will need ssh access to the host to forward to
        // for example: ssh -L 8983:localhost:8983 cn-stage-ucsb-1.test.dataone
        String solrCoreName = "search_core";
        SolrJClient client = new SolrJClient("http://localhost:8983/solr/" + solrCoreName);
        client.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
       
        
        
        SolrElementAdd data = new SolrElementAdd();

        Identifier id = TypeFactory.buildIdentifier("foo-" + System.currentTimeMillis());
        
        SystemMetadata smd = new SystemMetadata(); 
        smd.setIdentifier(id);
        System.out.println(id.getValue());

        try {
            
            List<SolrDoc> results = client.getDocumentBySolrId("", "tao.2013022113480472890");  
            
            
//            ByteArrayOutputStream baos = new ByteArrayOutputStream();
//            TypeMarshaller.marshalTypeToOutputStream(smd, baos);
//            InputStream is = IOUtils.toInputStream(baos.toString());
//
//            indexService.parseTaskObject(id.getValue(), is, null);
//
//            List<SolrDoc> docs = new ArrayList<SolrDoc>();

            
            data.setDocList(results);

            client.sendUpdate(null, data);
            
//        } catch (XPathExpressionException | SAXException
//                | ParserConfigurationException | EncoderException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        } catch (MarshallingException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
        } finally {}
       
    }
    
    
    
}
