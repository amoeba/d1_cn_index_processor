package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.dataone.configuration.Settings;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SolrJClientIT {

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
            List<SolrDoc> results = client.getDocumentBySolrId("", "14651b180c80823137427714ec2168035c953f61");     
            assertTrue("SHould get a result to parse", results.size() > 0);
            for (SolrDoc sd : results) {
                sd.serialize(System.out, "UTF-8");
                System.out.println();
            }
        } catch (NullPointerException npe) {
            npe.printStackTrace();
        }
        
    }
    
}
