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

import org.apache.commons.lang.StringUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Assert;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

/**
 * Solr unit test framework is dependent on JUnit 4.7. Later versions of junit
 * will break the base test classes.
 * 
 * @author sroseboo
 * 
 */
public abstract class DataONESolrJettyTestBase extends SolrJettyTestBase {

    protected ApplicationContext context;
    protected ArrayList<XPathDocumentParser> documentParsers;

    protected void addEmlToSolrIndex(Resource sysMetaFile) throws Exception {
        XPathDocumentParser parser = getXPathDocumentParser();
        SystemMetadata smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                sysMetaFile.getInputStream());
        // path to actual science metadata document
        String path = StringUtils.remove(sysMetaFile.getFile().getPath(), "/SystemMetadata");
        parser.process(smd.getIdentifier().getValue(), sysMetaFile.getInputStream(), path);
    }

    protected void addSysAndSciMetaToSolrIndex(Resource sysMeta, Resource sciMeta) throws Exception {
        XPathDocumentParser parser = getXPathDocumentParser();
        SystemMetadata smd = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                sysMeta.getInputStream());
        String path = sciMeta.getFile().getAbsolutePath();
        parser.process(smd.getIdentifier().getValue(), sysMeta.getInputStream(), path);
    }

    protected SolrDocument assertPresentInSolrIndex(String pid) throws SolrServerException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "id:" + ClientUtils.escapeQueryChars(pid));
        QueryResponse qr = getSolrServer().query(solrParams);
        Assert.assertFalse(qr.getResults().isEmpty());
        SolrDocument result = qr.getResults().get(0);
        String id = (String) result.getFieldValue("id");
        Assert.assertEquals(pid, id);
        return result;
    }

    protected SolrDocumentList findByField(String field, String value) throws SolrServerException {
        return findByQueryString(field + ":" + value);
    }

    protected SolrDocumentList findByQueryString(String query) throws SolrServerException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", query);
        QueryResponse qr = getSolrServer().query(solrParams);
        return qr.getResults();
    }

    public void sendSolrDeleteAll() throws SolrServerException, IOException {
        getSolrServer().deleteByQuery("*:*");
    }

    protected void assertNotPresentInSolrIndex(String pid) throws SolrServerException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "id:" + pid);
        QueryResponse qr = getSolrServer().query(solrParams);
        Assert.assertTrue(qr.getResults().isEmpty());
    }

    protected SolrDocumentList getAllSolrDocuments() throws SolrServerException {
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "*:*");
        QueryResponse qr = getSolrServer().query(solrParams);
        return qr.getResults();
    }

    public void setUp() throws Exception {
        super.setUp();
        loadSpringContext();
        startJettyAndSolr();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected void loadSpringContext() {
        if (context == null) {
            context = new ClassPathXmlApplicationContext("org/dataone/cn/index/test-context.xml");
        }
        documentParsers = (ArrayList) context.getBean("documentParsers");
    }

    protected void startJettyAndSolr() throws Exception {
        if (jetty == null) {
            File f = new File(".");
            String localPath = f.getAbsolutePath();
            createJettyWithPort(
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
    protected static JettySolrRunner createJettyWithPort(String solrHome, String configFile,
            String context) throws Exception {
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

    protected XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }

}
