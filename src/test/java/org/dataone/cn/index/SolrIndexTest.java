package org.dataone.cn.index;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.DateTimeMarshaller;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;

/**
 * Solr unit test framework is dependent on JUnit 4.7. Later versions of junit
 * will break the base test classes.
 * 
 * @author sroseboo
 * 
 */
public class SolrIndexTest extends SolrJettyTestBase {

    private static Logger logger = Logger.getLogger(SolrIndexTest.class.getName());

    private ApplicationContext context;

    private ArrayList<XPathDocumentParser> documentParsers;

    // TODO: test resource map / data packaging index properties?

    @Test
    public void testSystemMetadataAndEml210ScienceData() throws Exception {
        // peggym.130.4 system metadata document for eml2.1.0 science metadata
        // document
        String pid = "peggym.130.4";
        Resource systemMetadataResource = (Resource) context.getBean("systemMetadataResource4");

        // add peggym.130.4 to solr index, using XPathDocumentParser (used by
        // index-task-processor)
        generateIndexData(systemMetadataResource);

        // retrieve solrDocument for peggym130.4 from solr server by pid
        SolrDocument result = getSolrDocument(pid);

        // test science metadata fields in eml210 config match actual fields in
        // solr index document
        ScienceMetadataDocumentSubprocessor eml210 = (ScienceMetadataDocumentSubprocessor) context
                .getBean("eml210Subprocessor");

        Resource scienceMetadataResource = (Resource) context.getBean("scienceMetadataResource4");
        Document scienceMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                scienceMetadataResource.getInputStream());
        for (SolrField field : eml210.getFieldList()) {
            processFields(result, scienceMetadataDoc, field);
        }

        // test system metadata fields in system metadata config match those
        // in solr index document
        Document systemMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                systemMetadataResource.getInputStream());
        for (SolrField field : getXPathDocumentParser().getFields()) {
            processFields(result, systemMetadataDoc, field);
        }
    }

    private SolrDocument getSolrDocument(String pid) throws SolrServerException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "id:" + pid);
        QueryResponse qr = getSolrServer().query(solrParams);
        Assert.assertFalse(qr.getResults().isEmpty());
        SolrDocument result = qr.getResults().get(0);
        String id = (String) result.getFieldValue("id");
        Assert.assertEquals(pid, id);
        return result;
    }

    private void processFields(SolrDocument result, Document metadataDoc, SolrField field)
            throws Exception {
        List<SolrElementField> fields = field.getFields(metadataDoc);
        if (fields.isEmpty() == false) {
            SolrElementField docField = fields.get(0);
            Object solrValueObject = result.getFieldValue(docField.getName());

            System.out.println("Comparing value for field " + docField.getName());
            if (solrValueObject == null) {
                Assert.assertTrue(docField.getValue() == null || "".equals(docField.getValue()));
            } else if (solrValueObject instanceof String) {
                String solrValue = (String) solrValueObject;
                String docValue = docField.getValue();
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docField.getValue(), solrValue);
            } else if (solrValueObject instanceof Boolean) {
                Boolean solrValue = (Boolean) solrValueObject;
                Boolean docValue = Boolean.valueOf(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue, solrValue);
            } else if (solrValueObject instanceof Long) {
                Long solrValue = (Long) solrValueObject;
                Long docValue = Long.valueOf(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue, solrValue);
            } else if (solrValueObject instanceof Float) {
                Float solrValue = (Float) solrValueObject;
                Float docValue = Float.valueOf(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue, solrValue);
            } else if (solrValueObject instanceof Date) {
                Date solrValue = (Date) solrValueObject;
                Date docValue = DateTimeMarshaller.deserializeDateToUTC(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue.getTime(), solrValue.getTime());
            } else if (solrValueObject instanceof ArrayList) {
                ArrayList solrValueArray = (ArrayList) solrValueObject;
                ArrayList documentValueArray = new ArrayList();
                for (SolrElementField sef : fields) {
                    documentValueArray.add(sef.getValue());
                }
                System.out.println("Doc Value:  " + documentValueArray);
                System.out.println("Solr Value: " + solrValueArray);
                Assert.assertTrue(CollectionUtils.isEqualCollection(documentValueArray,
                        solrValueArray));
            } else {
                Assert.assertTrue(
                        "Unknown solr value object type for field: " + docField.getName(), false);
            }
            System.out.println("");
        }
    }

    private void generateIndexData(Resource sysMetaFile) throws Exception {
        XPathDocumentParser parser = getXPathDocumentParser();

        SystemMetadata smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                sysMetaFile.getInputStream());

        // path to actual science metadata document
        String path = StringUtils.remove(sysMetaFile.getFile().getPath(), "/SystemMetadata");
        parser.process(smd.getIdentifier().getValue(), sysMetaFile.getInputStream(), path);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        loadSpringContext();
        startJettyAndSolr();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void loadSpringContext() {
        if (context == null) {
            context = new ClassPathXmlApplicationContext("org/dataone/cn/index/test-context.xml");
        }
        documentParsers = (ArrayList) context.getBean("documentParsers");
    }

    private void startJettyAndSolr() throws Exception {
        if (jetty == null) {
            File f = new File(".");
            String localPath = f.getAbsolutePath();
            createJetty(
                    localPath + "/src/test/resources/org/dataone/cn/index/resources/solr",
                    localPath
                            + "/src/test/resources/org/dataone/cn/index/resources/solr/conf/solrconfig.xml",
                    null);
        }
        if (server == null) {
            getSolrServer();
        }
    }

    // method is copied in from SolrJettyTestBase in order to set the port
    // number to match solr.properties (8983) for XPathDocumentParser to connect
    // to same solr server. If left unset, the port number is a random open
    // port.
    public static JettySolrRunner createJetty(String solrHome, String configFile, String context)
            throws Exception {
        // creates the data dir
        initCore(null, null, solrHome);

        ignoreException("maxWarmingSearchers");

        // this sets the property for jetty starting SolrDispatchFilter
        System.setProperty("solr.solr.home", solrHome);
        System.setProperty("solr.data.dir", dataDir.getCanonicalPath());

        context = context == null ? "/solr" : context;
        SolrJettyTestBase.context = context;
        jetty = new JettySolrRunner(context, 8983, configFile);

        jetty.start();
        port = jetty.getLocalPort();
        log.info("Jetty Assigned Port#" + port);
        return jetty;
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }
}
