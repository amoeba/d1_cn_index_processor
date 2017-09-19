package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.io.IOUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.dataone.cn.index.messaging.MockMessagingClientConfiguration;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.configuration.Settings;
import org.dataone.exceptions.MarshallingException;
import org.dataone.service.types.v1.AccessPolicy;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.xml.sax.SAXException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml"})
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
        smd.setAccessPolicy(AccessUtil.addPublicAccess(null));
        smd.setAuthoritativeMemberNode(TypeFactory.buildNodeReference("urn:node:mnDemo5"));
        smd.setChecksum(new Checksum());
        smd.getChecksum().setAlgorithm("foo");
        smd.getChecksum().setValue("asdfghjklqwertyuio");
        smd.setDateSysMetadataModified(new Date());
        smd.setDateUploaded(new Date());
        smd.setFormatId(TypeFactory.buildFormatIdentifier("text/csv"));
        smd.setOriginMemberNode(TypeFactory.buildNodeReference("urn:node:mnDemo5"));
        smd.setRightsHolder(TypeFactory.buildSubject("CN=Jing Tao T6470,O=DataONE Test,C=US,DC=cilogon,DC=org"));
        smd.setSerialVersion(BigInteger.ONE);
        smd.setSize(new BigInteger("123456"));
        smd.setSubmitter(TypeFactory.buildSubject("CN=Jing Tao T6470,O=DataONE Test,C=US,DC=cilogon,DC=org"));

        
        try {
                                   
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(smd, baos);
            String serializedSmd = baos.toString();
            System.out.println(serializedSmd);
            InputStream is = IOUtils.toInputStream(serializedSmd);

            Map<String,SolrDoc> docs = indexService.parseTaskObject(id.getValue(), is, null);

            
            List<SolrDoc> docList = new ArrayList<SolrDoc>();
            
            for (Entry<String,SolrDoc> e: docs.entrySet()) {
                System.out.println("== adding: " + e.getKey());
                docList.add(e.getValue());
            }
            data.setDocList(docList);

            client.sendUpdate(null, data);
            client.getSolrClient().commit();
            
        } catch (XPathExpressionException | SAXException
                | ParserConfigurationException | EncoderException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MarshallingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SolrServerException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        } finally {}
       
    }
    
    
    
}
