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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.core.io.Resource;

import com.hazelcast.core.Hazelcast;

/**
 * Solr unit test framework is dependent on JUnit 4.7. Later versions of junit
 * will break the base test classes.
 * 
 * @author sroseboo
 * 
 */
public class SolrIndexReprocessTest extends DataONESolrJettyTestBase {

    private static Logger logger = Logger.getLogger(SolrIndexReprocessTest.class.getName());

    private IndexTaskProcessor processor;
    private IndexTaskGenerator generator;

    private Resource peggym1271Sys;
    private Resource peggym1281Sys;
    private Resource peggym1291Sys;
    private Resource peggym1304Sys;
    private Resource peggym1305Sys;
    private Resource peggymResourcemapSeriesSys;

    @BeforeClass
    public static void init() {
        HazelcastClientFactoryTest.startHazelcast();
    }

    @AfterClass
    public static void shutdown() throws Exception {
        Hazelcast.shutdownAll();
    }

    /**
     * Test reprocessing when new version of object in a data package is updated
     */
    @Test
    public void testReprocessDataPackage() throws Exception {
        // create/index data package
        deleteAll();
        indexTestDataPackage();
        //verify in index correct
        verifyTestDataPackageIndexed();
        indexNewRevision(peggym1305Sys);
        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNewRevision();
    }

    private void deleteAll() {
        HTTPService httpService = (HTTPService) context.getBean("httpService");
        httpService.sendSolrDelete("peggym.130.4");
        httpService.sendSolrDelete("peggym.130.5");
        httpService.sendSolrDelete("peggym.127.1");
        httpService.sendSolrDelete("peggym.128.1");
        httpService.sendSolrDelete("peggym.129.1");
        httpService.sendSolrDelete("peggym.resourcemap-series");
    }

    private void indexTestDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemapSeriesSys);
        processor.processIndexTaskQueue();
    }

    private void indexNewRevision(Resource resource) {
        addSystemMetadata(resource);
        processor.processIndexTaskQueue();
    }

    private void verifyDataPackageNewRevision() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-series",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.128.1");
        assertPresentInSolrIndex("peggym.129.1");

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(1,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-series",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(3, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        // check that the new revision also has the resource map value on it
        SolrDocument scienceMetadataRevision = assertPresentInSolrIndex("peggym.130.5");
        Assert.assertEquals(1, ((List) scienceMetadataRevision
                .getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-series", ((List) scienceMetadataRevision
                .getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        assertPresentInSolrIndex("peggym.resourcemap-series");

    }

    private void verifyTestDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        System.out.println("DATA=" + data);
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-series",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.128.1");
        assertPresentInSolrIndex("peggym.129.1");

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(1,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-series",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(3, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        assertPresentInSolrIndex("peggym.resourcemap-series");
    }

    private void addSystemMetadata(Resource systemMetadataResource) {
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    systemMetadataResource.getInputStream());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }
        String path = null;
        try {
            path = StringUtils
                    .remove(systemMetadataResource.getFile().getPath(), "/SystemMetadata");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        HazelcastClientFactory.getSystemMetadataMap().put(sysmeta.getIdentifier(), sysmeta);
        //sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);
        HazelcastClientFactory.getObjectPathMap().putAsync(sysmeta.getIdentifier(), path);
        generator.processSystemMetaDataUpdate(sysmeta, path);
    }

    public void setUp() throws Exception {
        super.setUp();
        configureSpringResources();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void configureSpringResources() {
        processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
        generator = (IndexTaskGenerator) context.getBean("indexTaskGenerator");

        peggym1271Sys = (Resource) context.getBean("peggym1271Sys");
        peggym1281Sys = (Resource) context.getBean("peggym1281Sys");
        peggym1291Sys = (Resource) context.getBean("peggym1291Sys");
        peggym1304Sys = (Resource) context.getBean("peggym1304Sys");
        peggym1305Sys = (Resource) context.getBean("peggym1305Sys");
        peggymResourcemapSeriesSys = (Resource) context.getBean("peggymResourcemapSeriesSys");

    }

}
