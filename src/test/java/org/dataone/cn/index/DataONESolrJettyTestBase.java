/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.parser.BaseReprocessSubprocessor;
import org.dataone.cn.indexer.parser.ISolrField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.DateTimeMarshaller;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Assert;
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
@SuppressSSL
public abstract class DataONESolrJettyTestBase extends SolrJettyTestBase {

    protected ApplicationContext context;
    private SolrIndexService solrIndexService;
    
    protected static Log __logger = LogFactory.getLog(DataONESolrJettyTestBase.class);

    protected void addEmlToSolrIndex(Resource sysMetaFile) throws Exception {
        SolrIndexService indexService = solrIndexService;
        SystemMetadata smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                sysMetaFile.getInputStream());
        // path to actual science metadata document
        String path = StringUtils.remove(sysMetaFile.getFile().getPath(), File.separator + "SystemMetadata");
        indexService.insertIntoIndex(smd.getIdentifier().getValue(), sysMetaFile.getInputStream(),
                path);
    }

    protected void addSysAndSciMetaToSolrIndex(Resource sysMeta, Resource sciMeta) throws Exception {
        SolrIndexService indexService = solrIndexService;
        SystemMetadata smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                sysMeta.getInputStream());
        String path = sciMeta.getFile().getAbsolutePath();
        indexService
                .insertIntoIndex(smd.getIdentifier().getValue(), sysMeta.getInputStream(), path);
    }

    protected SolrDocument assertPresentInSolrIndex(String pid) throws SolrServerException,
            IOException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        // use a real-time get to avoid commit issues
        SolrDocument result = getSolrClient().getById(pid);
        Assert.assertFalse("Solr response should not be empty for id query for " + pid , result.isEmpty());

//        solrParams.set("q", "id:" + ClientUtils.escapeQueryChars(pid));
//        solrParams.set("fl", "*");
//        QueryResponse qr = getSolrClient().query(solrParams);
//        Assert.assertFalse(qr.getResults().isEmpty());
//        SolrDocument result = qr.getResults().get(0);
        
        String id = (String) result.getFieldValue("id");
        Assert.assertEquals("Solr response should have the pid in the 'id' field", pid, id);
        return result;
    }

    protected SolrDocumentList findByField(String field, String value) throws SolrServerException,
            IOException {
        return findByQueryString(field + ":" + value);
    }

    protected SolrDocumentList findByQueryString(String query) throws SolrServerException,
            IOException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", query);
        QueryResponse qr = getSolrClient().query(solrParams);
        return qr.getResults();
    }

    public void sendSolrDeleteAll() throws SolrServerException, IOException {
        UpdateResponse response = getSolrClient().deleteByQuery("*:*");
        getSolrClient().commit();
        __logger.warn("Request URI: " + response.getRequestUrl());
        __logger.warn("Response size: " + response.getResponse().size());
        __logger.warn("First response result: " + response.getResponse().getName(0) + " = " +  response.getResponse().getVal(0).toString());
        __logger.warn("Response status code: " + response.getStatus());
        __logger.warn("Deleted All...");
        
    }

    protected void assertNotPresentInSolrIndex(String pid) throws SolrServerException, IOException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "id:" + pid);
        QueryResponse qr = getSolrClient().query(solrParams);
//        Assert.assertEquals(pid + " should return 0 results on an identity query", 0, qr.getResults().size());
        if (qr.getResults().size() > 0) {
            SolrDocument sd = qr.getResults().get(0);
            Assert.assertNull("a fully formed record for the identifier " + pid + " should not be retrievable", sd.getFieldValue("dateUploaded"));
//            Assert.assertEquals(pid + " should not be retrievable from the index.", null, StringUtils.join(sd.keySet(),", "));
        }
    }

    protected SolrDocumentList getAllSolrDocuments() throws SolrServerException, IOException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "*:*");
        QueryResponse qr = getSolrClient().query(solrParams);
        return qr.getResults();
    }

    protected void compareFields(SolrDocument solrResult, Document metadataDoc,
            ISolrField fieldToCompare, String identifier) throws Exception {
        List<SolrElementField> fields = fieldToCompare.getFields(metadataDoc, identifier);
        if (fields.isEmpty() == false) {
            SolrElementField docField = fields.get(0);
            Object solrValueObject = solrResult.getFieldValue(docField.getName());

            System.out.println("Comparing value for field " + docField.getName());
            if (solrValueObject == null) {
                if (!"text".equals(docField.getName())) {
                    System.out.println("Null result value for field name:  " + docField.getName()
                            + ", actual: " + docField.getValue());
                    Assert.assertTrue(docField.getValue() == null || "".equals(docField.getValue()));
                }
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
            } else if (solrValueObject instanceof Integer) {
                Integer solrValue = (Integer) solrValueObject;
                Integer docValue = Integer.valueOf(docField.getValue());
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

    public void setUp() throws Exception {
        super.setUp();
        loadSpringContext();
        __logger.info("LoadedSpringContext...");
        startJettyAndSolr();
       
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected void loadSpringContext() {
        if (context == null) {
            context = new ClassPathXmlApplicationContext("/org/dataone/cn/index/test-context.xml");
        }
        solrIndexService = (SolrIndexService) context.getBean("solrIndexService");
    }

    protected void startJettyAndSolr() throws Exception {
        if (jetty == null) {
            JettyConfig jconfig = JettyConfig.builder().setPort(8983).build();
            File f = new File(".");
            String localPath = f.getAbsolutePath();
            createJettyWithPort(localPath
                    + "/src/test/resources/org/dataone/cn/index/resources/solr5home", jconfig);
            __logger.info("Started Jetty and Solr...");
        } else {
            __logger.warn("Jetty already started...");
        }
    }

    // method is copied in from SolrJettyTestBase in order to set the port
    // number to match solr.properties (8983) for XPathDocumentParser to connect
    // to same solr server. If left unset, the port number is a random open
    // port.
    protected static JettySolrRunner createJettyWithPort(String solrHome, JettyConfig config)
            throws Exception {
        createJetty(solrHome, config);
        return jetty;
    }
}
