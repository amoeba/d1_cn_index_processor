package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.dataone.cn.hazelcast.HazelcastConfigLocationFactory;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.parser.SubprocessorUtility;
import org.dataone.configuration.Settings;
import org.dataone.exceptions.MarshallingException;
import org.dataone.service.types.v1.Checksum;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v1.util.AccessUtil;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.xml.sax.SAXException;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "it-test-context.xml"})
public class SolrJClientIT {

    public static final Log logger = LogFactory.getLog(SolrJClientIT.class);

    static String hzConfigLocation = Settings.getConfiguration().getString("dataone.hazelcast.location.processing.clientconfig");
    static HazelcastInstance hzInstance; 
    
    @Autowired
    SolrIndexService indexService;
    
    @Autowired
    SolrJClient d1IndexerSolrClient;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
      
        String cLoc = HazelcastConfigLocationFactory.getStorageConfigLocation();
        logger.info("Hazelcast configuration location: " + cLoc);
        if (cLoc != null && cLoc.startsWith("classpath:")) 
        {
            cLoc = cLoc.replace("classpath:", "");
        }
        ClasspathXmlConfig config = new ClasspathXmlConfig(cLoc);

        hzInstance = Hazelcast.newHazelcastInstance(config);

    }
    
    @AfterClass
    public static void setUpAfterClass() throws InterruptedException {
        Thread.sleep(2000);
        hzInstance.getLifecycleService().shutdown();
    }

    @Before
    public void setUp() throws Exception {
        // test requires port-forwarding unless you have a populated solr instance running locally 
        // of course, you will need ssh access to the host to forward to
        // for example: ssh -L 8983:localhost:8983 cn-dev-ucsb-1.test.dataone.org
//        String solrCoreName = "search_core";
//        this.client = new SolrJClient("http://localhost:8983/solr/" + solrCoreName);
//        this.client.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
       indexService.setD1IndexerSolrClient(this.d1IndexerSolrClient);
    }

    @Test
    public void testTrue() {
        assertTrue("Should be true", true);
    }
   
    @Ignore ("this is an integration test")
    @Test
    public void parseResponse() throws XPathExpressionException, IOException, EncoderException, SolrServerException {
        
        
        SolrClient bareClient = new HttpSolrClient.Builder("http://localhost:8983/solr/search_core").build();
        // this is a known object in cn-stage
        QueryResponse qr = bareClient.query(new SolrQuery("id:14651b180c80823137427714ec2168035c953f61"));
        
        try {
            List<SolrDoc> results = this.d1IndexerSolrClient.parseResponse(qr.getResults());      
        
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
        
        
        try {
            List<SolrDoc> results = this.d1IndexerSolrClient.getDocumentBySolrId("", "tao.2013022113480472890");  
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
        SolrClient sc = new HttpSolrClient.Builder("http://localhost:8983/solr/" + solrCoreName).build();
        SolrJClient client = new SolrJClient(sc);
        client.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
       
        

        Identifier id = TypeFactory.buildIdentifier("foo-" + System.currentTimeMillis());
        
        SystemMetadata smd = buildSystemMetadata();
        System.out.println(smd.getIdentifier().getValue());
        
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


            client.sendUpdate(null, docList);
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
    // a test to see if two diverging updates interfere with each other.  The scenario is two channels read the same record, alter it, then submit updates to solr.
    // the second update should not overwrite the first.
    @Ignore(" this is an integration test")
    @Test
    public void testRaceWrites() 
            throws XPathExpressionException, MarshallingException, IOException, SAXException, ParserConfigurationException, EncoderException, InstantiationException, IllegalAccessException, InterruptedException {

        // test that divergent changes are not both accepted

        SubprocessorUtility su = new SubprocessorUtility();

        // 1. create the original in the index 
        SystemMetadata original = buildSystemMetadata();

        System.out.println("Identifier = " + original.getIdentifier().getValue());

        List<SolrDoc> docs = smdToSolrDocList(original);

        this.d1IndexerSolrClient.sendUpdate(null, docs);

        System.out.println("Created original object");

        Thread.sleep(60);

        // 2. simulate two threads processing two tasks on the same object
        List<SolrDoc> queryOriginal1 = this.d1IndexerSolrClient.getDocumentBySolrId(null, original.getIdentifier().getValue());
        int originalSize = queryOriginal1.get(0).getFieldList().size();
        List<SolrDoc> queryOriginal2 = this.d1IndexerSolrClient.getDocumentBySolrId(null, original.getIdentifier().getValue());

        // 3. make the first update
        {
            SolrElementField sef = new SolrElementField();
            sef.setName("replicaMN");
            sef.setValue("urn:node:testFAKE");

            SolrDoc d = queryOriginal1.get(0);
            Iterator<SolrElementField> it = d.getFieldList().iterator();
//            while (it.hasNext()) {
//                SolrElementField s = it.next();
//                if (!s.getName().equals("id")) {
//                    it.remove();
//                }
//            }
//            System.out.println("==");
//            assertTrue("Should only have one field", d.getFieldList().size() == 1);
            d.addField(sef);

            System.out.println("==========  >>>  about to send the first update...");
            this.d1IndexerSolrClient.sendUpdate(null, queryOriginal1);
        }

        Thread.sleep(60);

        List<SolrDoc> results1 = this.d1IndexerSolrClient.getDocumentBySolrId(null, original.getIdentifier().getValue());         
        List<String> replicas = results1.get(0).getAllFieldValues("replicaMN");
        assertTrue("Should have at least as many fields as before", results1.get(0).getFieldList().size() >= originalSize);
        assertTrue("Should have testFAKE replica", replicas.contains("urn:node:testFAKE"));
        
        // 4. make the second update
        {
            SolrElementField sef = new SolrElementField();
            sef.setName("replicaMN");
            sef.setValue("urn:node:testPHONY");

            queryOriginal2.get(0).addField(sef);
            try {
                this.d1IndexerSolrClient.sendUpdate(null, queryOriginal2);
            } catch (IOException e) {
                e.printStackTrace();
                throw e;
            }
        }
        Thread.sleep(60);

        List<SolrDoc> results2 = this.d1IndexerSolrClient.getDocumentBySolrId(null, original.getIdentifier().getValue());


        results2.get(0).serialize(System.out, "UTF-8");
        
        // 5. should have either failed or incorporated both changes...
        List<String> replicas2 = results2.get(0).getAllFieldValues("replicaMN");
        assertTrue("Should have at least as many fields as before", results1.get(0).getFieldList().size() >= originalSize);
        assertTrue("SHould have testFAKE replica", replicas2.contains("urn:node:testFAKE"));
    }
        
        

   @Ignore ("this is an integration test")
   @Test
    public void testTypicalPackageSynchronization() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException, EncoderException, InterruptedException {
  

        String mdPid = "IndexerIT.md." + System.currentTimeMillis();
        String dataPid = "IndexerIT.data." + System.currentTimeMillis();
        String resMapPid = "IndexerIT.resMap." + System.currentTimeMillis();
        
        System.out.println(String.format("Identifiers: %s,  %s,   %s", mdPid, dataPid, resMapPid));
        
        String mdPidPattern = new String("tao.13243.1");
        String dataPidPattern = new String("tao.13242.1");
        String resMapPattern = new String("resourceMap_tao.13243.1");
        // since the MD document is a substring of the resourceMap, we need to put the resourceMap at the front of the list!
        String[] replacements = new String[]{resMapPid,mdPid,dataPid};
        String[] originals = new String[]{resMapPattern,mdPidPattern,dataPidPattern};
        
        System.out.println(dataPid);
        InputStream stream = replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-0.xml"), originals, replacements);
        
        // submit the data object
        try {

           
            
            
            submitUpdate(dataPid,
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-0.xml"), originals, replacements), 
                    null);

            Thread.sleep(60);

            List<SolrDoc> queryResults = this.d1IndexerSolrClient.getDocumentBySolrId(null, dataPid);
            assertTrue("data object should be queryable",queryResults.get(0).getField("id").getValue().equals(dataPid));


            // submit the metadata object
            submitUpdate(new String(mdPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13243.1-0.xml"), originals, replacements), 
                    this.getClass().getResource("./tao.13243.1.object.xml").getFile());



                    // update the data object        
                    submitUpdate(new String(dataPid),
                            replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-1.xml"), originals, replacements), null);
                    
                    // update the metadata object    
                    submitUpdate(new String(mdPid),
                            replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13243.1-1.xml"), originals, replacements), null);
                    
                    
            
                    // update the data object
                    submitUpdate(new String(dataPid),
                            replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-2.xml"), originals, replacements), null);
            
                    // update the metadata object    
                    submitUpdate(new String(mdPid),
                            replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13243.1-2.xml"), originals, replacements), null);
                         
                    

            
            InputStream resMap = replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1.rdf"), originals, replacements);
            java.io.File file = java.io.File.createTempFile("SolrJClientIT.", "rdf");
            FileOutputStream fos = new FileOutputStream(file);
            IOUtils.copy(resMap, fos);
            fos.flush();
            fos.close();

            submitUpdate(new String(resMapPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/resourceMap_tao.13243.1-0.xml"), originals, replacements), 
                    file.getAbsolutePath());

                    submitUpdate(new String(resMapPid),
                            replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/resourceMap_tao.13243.1-1.xml"), originals, replacements), null);
            
                    
                    submitUpdate(new String(resMapPid),
                            replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/resourceMap_tao.13243.1-2.xml"), originals, replacements), null);
               
            
  

            List<SolrDoc> d = this.d1IndexerSolrClient.getDocumentBySolrId(null, dataPid);
            List<SolrDoc> md = this.d1IndexerSolrClient.getDocumentBySolrId(null, mdPid);
            List<SolrDoc> rm = this.d1IndexerSolrClient.getDocumentBySolrId(null, resMapPid);
            
            
            assertNotNull("Data index doc should have a resourceMap field", d.get(0).getFirstFieldValue("resourceMap"));
            assertNotNull("Data index doc should have a resourceMap field", d.get(0).getFirstFieldValue("isDocumentedBy"));
            assertNotNull("Metadata index doc should have a resourceMap field", md.get(0).getFirstFieldValue("resourceMap"));
            assertNotNull("Metadata index doc should have a resourceMap field", md.get(0).getFirstFieldValue("documents"));

            
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
   }
    
    private void submitUpdate(String id, InputStream sysMeta, String objectPath) 
            throws XPathExpressionException, IOException, SAXException, ParserConfigurationException, EncoderException, InterruptedException {

        String path = objectPath != null ? objectPath : null;
 //       Map<String,SolrDoc> docs = indexService.parseTaskObject(id, sysMeta, path);
        List<SolrDoc> updates = indexService.processObject(id, sysMeta, path);

//        List<SolrElementAdd> updates = new ArrayList<SolrElementAdd>();
//
//        for (Entry<String,SolrDoc> e: docs.entrySet()) {
//            System.out.println("==== adding: " + e.getKey());
//            System.out.println("   +======> " + e.getValue());
//            docList.add(e.getValue());
//        }
        this.d1IndexerSolrClient.sendUpdate(null,updates);
        Thread.sleep(60);
    }
  
    
    
 //////////////////   ////////////////   ////////////////   ////////////////   ////////////////   ////////////////   ////////////////   
    
    
  
    private SystemMetadata buildSystemMetadata() {
        
        SystemMetadata smd = new SystemMetadata(); 
        smd.setIdentifier(TypeFactory.buildIdentifier("TestSolrUpdate-" + System.currentTimeMillis()));
        smd.setAccessPolicy(AccessUtil.addPublicAccess(null));
        smd.setAuthoritativeMemberNode(TypeFactory.buildNodeReference("urn:node:mnDemo5"));
        smd.setChecksum(new Checksum());
        smd.getChecksum().setAlgorithm("foo");
        smd.getChecksum().setValue("asdfghjklqwertyuio");
        smd.setDateSysMetadataModified(new Date());
        smd.setDateUploaded(new Date());
        smd.setFormatId(TypeFactory.buildFormatIdentifier("text/csv"));
        smd.setOriginMemberNode(TypeFactory.buildNodeReference("urn:node:mnDemo5"));
        smd.setRightsHolder(TypeFactory.buildSubject("CN=Rob Nahf,O=DataONE Test,C=US,DC=cilogon,DC=org"));
        smd.setSerialVersion(BigInteger.ONE);
        smd.setSize(new BigInteger("123456"));
        smd.setSubmitter(TypeFactory.buildSubject("CN=Rob Nahf,O=DataONE Test,C=US,DC=cilogon,DC=org"));
        return smd;
    }
    
    
    
    private List<SolrDoc> smdToSolrDocList(SystemMetadata smd) 
            throws MarshallingException, IOException, XPathExpressionException, SAXException, ParserConfigurationException, EncoderException {
        
        
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(smd, baos);
            String serializedSmd = baos.toString();
            InputStream is = IOUtils.toInputStream(serializedSmd);

            Map<String,SolrDoc> docs = indexService.parseTaskObject(smd.getIdentifier().getValue(), is, null);

            
            List<SolrDoc> docList = new ArrayList<SolrDoc>();
            
            for (Entry<String,SolrDoc> e: docs.entrySet()) {
                System.out.println("== adding: " + e.getKey());
                docList.add(e.getValue());
            }
            return docList;

    }
    
    
    private InputStream replaceIdentifiers(InputStream is, String[] original, String[] replacement) throws IOException {
        
        String input = IOUtils.toString(is);
        for (int i=0; i< original.length; i++) {
            input = input.replaceAll(original[i], replacement[i]);
        }
        return IOUtils.toInputStream(input);
        
    }
   
}
