package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.dataone.cn.hazelcast.HazelcastConfigLocationFactory;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.SolrIndexService;
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
@ContextConfiguration(locations = { "../../index/test-context.xml"})
public class SolrUpdatePerformanceIT {

    public static final Log logger = LogFactory.getLog(SolrUpdatePerformanceIT.class);

    static String hzConfigLocation = Settings.getConfiguration().getString("dataone.hazelcast.location.processing.clientconfig");
    static HazelcastInstance hzInstance; 
    
    @Autowired
    SolrIndexService indexService;
    
    @Autowired
    D1IndexerSolrClient d1IndexerSolrClient;
    
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
    public void update() throws IOException { //XPathExpressionException, IOException, EncoderException, SolrServerException {
        
        // test requires port-forwarding unless you have a populated solr instance running locally 
        // of course, you will need ssh access to the host to forward to
        // for example: ssh -L 8983:localhost:8983 cn-stage-ucsb-1.test.dataone
        String solrCoreName = "search_core";
        SolrClient sc = new HttpSolrClient("http://localhost:8983/solr/" + solrCoreName);
        D1IndexerSolrClient client = new SolrJClient(sc);
 //       client = d1IndexerSolrClient;      
        
        client.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
       
        String idSeries = "solrUpdateTestSeries-" + System.currentTimeMillis();
        
        int iterations = 10;
        int elapsedTime = 0;
        for (int i = 0;i<iterations; i++) {
            SolrElementAdd data = new SolrElementAdd();

            Identifier id = TypeFactory.buildIdentifier(idSeries + "-" + i);

            SystemMetadata smd = buildSystemMetadata();
            System.out.println(smd.getIdentifier().getValue());

            try {

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                TypeMarshaller.marshalTypeToOutputStream(smd, baos);
                String serializedSmd = baos.toString();
//                System.out.println(serializedSmd);
                InputStream is = IOUtils.toInputStream(serializedSmd);

                Map<String,SolrDoc> docs = indexService.parseTaskObject(id.getValue(), is, null);


                List<SolrDoc> docList = new ArrayList<SolrDoc>();

                for (Entry<String,SolrDoc> e: docs.entrySet()) {
                    System.out.println("== adding: " + e.getKey());
                    docList.add(e.getValue());
                }
                data.setDocList(docList);
                
                long t0 = System.currentTimeMillis();
                client.sendUpdate(client.getSolrIndexUri()+"/update/?commit=true", data);
//                client.getSolrClient().commit();
                elapsedTime += (System.currentTimeMillis() - t0);
                
            } catch (XPathExpressionException | SAXException
                    | ParserConfigurationException | EncoderException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (MarshallingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
//            } catch (SolrServerException e1) {
                // TODO Auto-generated catch block
  //              e1.printStackTrace();
            } finally {}
        }
        System.out.println("========================");
        System.out.println(" iterations: " + iterations);
        System.out.println(" avg. time ms: " + elapsedTime / iterations);
        System.out.println("========================");
 
        if (client instanceof SolrJClient) {
            SolrJClient cl = (SolrJClient) client;
            int i = 0;
            int updateTtot = 0;
            int updateN = 0;
            int queryTtot = 0;
            int queryN = 0;
            try {
                while(true) {
                    if (cl.solrCallList.get(i).startsWith("update")) {
                        updateN++;
                        updateTtot += cl.solrCallDurationList.get(i);
                    } else {
                        queryN++;
                        queryTtot += cl.solrCallDurationList.get(i);
                    }
                    System.out.println(String.format("%s\t%s\t%s",
                        cl.solrCallList.get(i),
                        cl.solrCallDurationList.get(i),
                        cl.solrCallStartTimeList.get(i++)));
                } 
            }
            catch (IndexOutOfBoundsException e) {
                ;
            }
            System.out.println("===========================");
            System.out.println("Queries: " + queryN);
            System.out.println("   Total time: " + queryTtot);
            System.out.println("   Avg time: " + (queryN == 0 ? 0 : queryTtot / queryN));
            System.out.println();
            System.out.println("===========================");
            System.out.println("Updates: " + updateN);
            System.out.println("   Total time: " + updateTtot);
            System.out.println("   Avg time: " + (updateN == 0 ? 0 : updateTtot / updateN));
            System.out.println("   commit within (ms): " + cl.COMMIT_WITHIN_MS);
            System.out.println();
            
        }
    }

        
        

    @Ignore ("scale up the package size ")
    @Test
    public void testTypicalPackageSynchronization() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException, EncoderException, InterruptedException {
  
//       String solrCoreName = "search_core";
//       SolrClient sc = new HttpSolrClient("http://localhost:8983/solr/" + solrCoreName);
//       d1IndexerSolrClient = new SolrJClient(sc);     
//       
//       d1IndexerSolrClient.setSolrSchemaPath(Settings.getConfiguration().getString("solr.schema.path"));
//       
        ((SolrJClient)d1IndexerSolrClient).COMMIT_WITHIN_MS = 10;
       
       // this parameter slows down the data object update rates - I could be swamping local network (comcast)
        long safeFollowingDistance = 10; 
       
        int dataMemberCount = 10;
       
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
            List<String> dataIdList = new ArrayList<>();
            // create the original data records in the index
            for (int d=1; d<=dataMemberCount; d++) {
                System.out.println("==============================================================================");
                String id = dataPid + "." + d;
                dataIdList.add(id);
                submitUpdate(id,
                        replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-0.xml"), 
                                new String[]{dataPidPattern}, new String[]{id}), 
                                null);
                Thread.sleep(safeFollowingDistance);
            }

            
            // create the original metadata object in the index
            System.out.println("==============================================================================");

            submitUpdate(new String(mdPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13243.1-0.xml"), originals, replacements), 
                    this.getClass().getResource("./tao.13243.1.object.xml").getFile());


            // update the data objects with new replica information     
            for (int d=1; d<=dataMemberCount; d++) {
                System.out.println("==============================================================================");
                String id = dataPid + "." + d;
                submitUpdate(new String(id),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-1.xml"), 
                            new String[]{dataPidPattern}, new String[]{id}), 
                            null);
                Thread.sleep(safeFollowingDistance);
            }

            // update the metadata object with new replica information
            System.out.println("==============================================================================");   
            submitUpdate(new String(mdPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13243.1-1.xml"), originals, replacements),
                    this.getClass().getResource("./tao.13243.1.object.xml").getFile());


            
            // update the data objects with more new replica information     
            for (int d=1; d<=dataMemberCount; d++) {
                System.out.println("==============================================================================");
                String id = dataPid + "." + d;
                submitUpdate(new String(id),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13242.1-2.xml"), 
                            new String[]{dataPidPattern}, new String[]{id}), 
                            null);
                Thread.sleep(safeFollowingDistance);
            }

            // update the metadata object with new replica information
            System.out.println("==============================================================================");  
            submitUpdate(new String(mdPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/tao.13243.1-2.xml"), originals, replacements),
                    this.getClass().getResource("./tao.13243.1.object.xml").getFile());


            System.out.println("==============================================================================");
            System.out.println("==============================================================================");
           
            File file = null;
            LineNumberReader lr = null;
            LineNumberReader lr2 = null;
            try
            {
                // build the head            
                InputStream resMap = replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-head.rdf"), originals, replacements);
                file = File.createTempFile("SolrUpdatePerformanceIT.", "rdf");
                FileOutputStream fos = new FileOutputStream(file);
                IOUtils.copy(resMap, fos);

                // add data statements
                for (String id : dataIdList) {
                    InputStream dataStatements = replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-dataDesc.rdf"),
                            new String[]{resMapPattern, mdPidPattern, dataPidPattern}, new String[]{resMapPid,mdPid,id});
                    IOUtils.copy(dataStatements, fos);
                }
                
                // such a hack...
                // add the metadata descriptions, but need to duplicate the 4th line for every data object in the package
                lr = new LineNumberReader(new InputStreamReader(replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-mdDesc.rdf"),
                            new String[]{resMapPattern, mdPidPattern, dataPidPattern}, new String[]{resMapPid,mdPid,dataPid})));
                String line = lr.readLine();
                while (!line.contains("documents")) {
                    IOUtils.write(line, fos);
                    fos.write("\n".getBytes());
                    line = lr.readLine();
                }
                for (String id : dataIdList) {
                    String l = line.replaceAll(dataPid, id);
                    IOUtils.write(l, fos);
                    fos.write("\n".getBytes());
                }
                while (line != null) {
                    line = lr.readLine();
                    IOUtils.write(line, fos);
                    fos.write("\n".getBytes());
                }

                // same such hack...
                // add the aggregation descriptions, but need to duplicate the 5th line for every data object in the package
                lr2 = new LineNumberReader(new InputStreamReader(replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-aggDesc.rdf"),
                            new String[]{resMapPattern, mdPidPattern, dataPidPattern}, new String[]{resMapPid,mdPid,dataPid})));
                line = lr2.readLine();
                while (!line.contains(dataPid)) {
                    IOUtils.write(line, fos);
                    fos.write("\n".getBytes());
                    line = lr2.readLine();
                }
                for (String id : dataIdList) {
                    String l = line.replaceAll(dataPid, id);
                    IOUtils.write(l, fos);
                    fos.write("\n".getBytes());
                }
                while (line != null) {
                    line = lr2.readLine();
                    IOUtils.write(line, fos);
                    fos.write("\n".getBytes());
                }
                
                
                // add the tail    
                InputStream tail = replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-tail.rdf"), originals, replacements);
                IOUtils.copy(tail, fos);
                
                fos.flush();
                fos.close();
            }
            finally {
                IOUtils.closeQuietly(lr);
                IOUtils.closeQuietly(lr2);
            }
            
            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
            System.out.println("ResourceMap: " + file.getAbsolutePath());
            System.out.println(IOUtils.toString(new FileInputStream(file), "UTF-8"));
            System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
            
            submitUpdate(new String(resMapPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/resourceMap_tao.13243.1-0.xml"), originals, replacements), 
                    file.getAbsolutePath());

            System.out.println("==============================================================================");
            submitUpdate(new String(resMapPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/resourceMap_tao.13243.1-1.xml"), originals, replacements), 
                    file.getAbsolutePath());

            System.out.println("==============================================================================");
            submitUpdate(new String(resMapPid),
                    replaceIdentifiers(this.getClass().getResourceAsStream("sysMeta/resourceMap_tao.13243.1-2.xml"), originals, replacements),
                    file.getAbsolutePath());


            if (d1IndexerSolrClient instanceof SolrJClient) {
                SolrJClient cl = (SolrJClient) d1IndexerSolrClient;
                int i = 0;
                int updateTtot = 0;
                int updateN = 0;
                int queryTtot = 0;
                int queryN = 0;
                try {
                    while(true) {
                        if (cl.solrCallList.get(i).startsWith("update")) {
                            updateN++;
                            updateTtot += cl.solrCallDurationList.get(i);
                        } else {
                            queryN++;
                            queryTtot += cl.solrCallDurationList.get(i);
                        }
                        System.out.println(String.format("%s\t%s\t%s",
                            cl.solrCallList.get(i),
                            cl.solrCallDurationList.get(i),
                            cl.solrCallStartTimeList.get(i++)));
                    } 
                }
                catch (IndexOutOfBoundsException e) {
                    ;
                }
                System.out.println("===========================");
                System.out.println("Queries: " + queryN);
                System.out.println("   Total time: " + queryTtot);
                System.out.println("   Avg time: " + (queryN == 0 ? 0 : queryTtot / queryN));
                System.out.println();
                System.out.println("===========================");
                System.out.println("Updates: " + updateN);
                System.out.println("   Total time: " + updateTtot);
                System.out.println("   Avg time: " + (updateN == 0 ? 0 : updateTtot / updateN));
                System.out.println("   commit within (ms): " + cl.COMMIT_WITHIN_MS);
                System.out.println();
                
            }

            Thread.sleep(15);
            
            List<SolrDoc> md = this.d1IndexerSolrClient.getDocumentBySolrId(null, mdPid);
            List<SolrDoc> rm = this.d1IndexerSolrClient.getDocumentBySolrId(null, resMapPid);
            
            assertNotNull("Metadata index doc should have a resourceMap field", md.get(0).getFirstFieldValue("resourceMap"));
            assertNotNull("Metadata index doc should have a documents field", md.get(0).getFirstFieldValue("documents"));
            assertTrue("Metadata index doc should have many values in documents field", md.get(0).getAllFieldValues("documents").size() == dataMemberCount);
            assertNotNull("ResourceMap index doc should exist", rm);
            assertTrue("ResourceMap index doc should exist", rm.size() == 1 );

            for (String id : dataIdList) {
                List<SolrDoc> d = this.d1IndexerSolrClient.getDocumentBySolrId(null, id);
                assertNotNull("Data index doc should have a resourceMap field ["+ id + "]", d.get(0).getFirstFieldValue("resourceMap"));
                assertNotNull("Data index doc should have an isDocumentedBy field ["+ id + "]", d.get(0).getFirstFieldValue("isDocumentedBy"));
                System.out.print(".");
            }
            System.out.println();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        
        
   }
    
    private void submitUpdate(String id, InputStream sysMeta, String objectPath) 
            throws XPathExpressionException, IOException, SAXException, ParserConfigurationException, EncoderException, InterruptedException {

        String path = objectPath != null ? objectPath : null;
 //       Map<String,SolrDoc> docs = indexService.parseTaskObject(id, sysMeta, path);
        SolrElementAdd updates = indexService.processObject(id, sysMeta, path);

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
    
    
    protected File buildResourceMap(String resMapId, String mdId, List<String> dataIds, String[] originals, String[] replacements) throws IOException {
        
      // build the head 
      InputStream resMap = replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-head.rdf"), originals, replacements);
      java.io.File file = java.io.File.createTempFile("SolrUpdatePerformanceIT.", "rdf");
      FileOutputStream fos = new FileOutputStream(file);
      IOUtils.copy(resMap, fos);

      // add data statements
      for (String id : dataIds) {
          InputStream dataStatements = replaceIdentifiers(this.getClass().getResourceAsStream("./resourceMap_tao.13243.1-head.rdf"),
                  new String[]{}, new String[]{id});
          IOUtils.copy(dataStatements, fos);
      }
      
      
      fos.flush();
      fos.close();

      return file;
    }
   
}
