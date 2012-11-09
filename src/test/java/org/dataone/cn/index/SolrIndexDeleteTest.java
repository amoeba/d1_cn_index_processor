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
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.Resource;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Solr unit test framework is dependent on JUnit 4.7. Later versions of junit
 * will break the base test classes.
 * 
 * @author sroseboo
 * 
 */
public class SolrIndexDeleteTest extends DataONESolrJettyTestBase {

    private static Logger logger = Logger.getLogger(SolrIndexDeleteTest.class.getName());

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");
    private static final String objectPathName = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");
    private HazelcastInstance hzMember;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private IMap<Identifier, String> objectPaths;

    private IndexTaskProcessor processor;
    private IndexTaskGenerator generator;

    private Resource peggym1271Sys;
    private Resource peggym1271SysArchived;
    private Resource peggym1281Sys;
    private Resource peggym1291Sys;
    private Resource peggym1304Sys;
    private Resource peggym1304SysArchived;
    private Resource peggymResourcemapSys;
    private Resource peggymResourcemapSysArchived;

    /**
     * Unit test of the HTTPService.sendSolrDelete(pid) method. Inserts record
     * into solr index using XPathDocumentParser. Does not use index task
     * generation/processing.
     **/
    @Test
    public void testHttpServiceSolrDelete() throws Exception {
        String pid = "peggym.130.4";
        Resource systemMetadataResource = (Resource) context.getBean("peggym1304Sys");
        deleteAll();
        addEmlToSolrIndex(systemMetadataResource);
        assertPresentInSolrIndex(pid);
        HTTPService httpService = (HTTPService) context.getBean("httpService");
        httpService.sendSolrDelete(pid);
        assertNotPresentInSolrIndex(pid);
    }

    /**
     * Adds and removes a single science metadata document to the solr index.
     * Uses index task processing/generation to process add/delete.
     * 
     * @throws Exception
     */
    @Test
    public void testDeleteSingleDocFromIndex() throws Exception {
        String pid = "peggym.130.4";
        deleteAll();
        addSystemMetadata(peggym1304Sys);
        processor.processIndexTaskQueue();
        assertPresentInSolrIndex(pid);
        addSystemMetadata(peggym1304SysArchived);
        processor.processIndexTaskQueue();
        assertNotPresentInSolrIndex(pid);
    }

    /**
     * Adds a data package (see indexTestDataPackage) to solr index and then
     * removes a science data document from the package and verifies the state
     * of the data package is correct with respect to hiding the archived data
     * doc. The resource map document is then updated and the package is
     * verified to ensure science data document is still not present in the
     * package.
     * 
     * @throws Exception
     */
    @Test
    public void testArchiveDataInPackage() throws Exception {
        // create/index data package
        deleteAll();
        indexTestDataPackage();
        // verify in index correct
        verifyTestDataPackageIndexed();
        // remove a data object by adding system metadata to task queue with
        // archive=true
        addSystemMetadata(peggym1271SysArchived);
        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNo1271();
        // update package object (resource map)
        addSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
        // verify again
        verifyDataPackageNo1271();
    }

    /**
     * Same scenario as testArchiveDataInPackage, but this time the file removed
     * is the science metadata document.
     * 
     * @throws Exception
     */
    @Test
    public void testArchiveScienceMetadataInPackage() throws Exception {
        // create/index data package
        deleteAll();
        indexTestDataPackage();
        // verify in index correct
        verifyTestDataPackageIndexed();
        // remove a data object by adding system metadata to task queue with
        // archive=true
        addSystemMetadata(peggym1304SysArchived);
        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNo1304();
        // update package object (resource map)
        addSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
        // verify again
        verifyDataPackageNo1304();
    }

    /**
     * Same scenario as testArchiveDataInPackage, but this time the data package
     * document itself is removed. This time the science metadata document is
     * updated and then the contents of the archived are verified.
     * 
     * @throws Exception
     */
    @Test
    public void testArchiveDataPackage() throws Exception {
        // create/index data package
        deleteAll();
        indexTestDataPackage();
        // verify in index correct
        verifyTestDataPackageIndexed();
        // remove a data object by adding system metadata to task queue with
        // archive=true
        addSystemMetadata(peggymResourcemapSysArchived);

        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNoResourceMap();
        // update package object (resource map)
        addSystemMetadata(peggym1304Sys);
        processor.processIndexTaskQueue();
        // verify again
        verifyDataPackageNoResourceMap();
    }

    /**
     * Verify that a data package will index when one (or more) documents in the
     * data package are archived.
     * 
     * @throws Exception
     */
    @Test
    public void testDataPackageWithArchivedDoc() throws Exception {
        deleteAll();
        indexTestDataPackageWithArchived1271Doc();
        verifyDataPackageNo1271();
    }

    private void verifyDataPackageNoResourceMap() throws Exception {
        assertPresentInSolrIndex("peggym.127.1");

        SolrDocument data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.129.1");
        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertNotPresentInSolrIndex("peggym.resourcemap");
    }

    private void verifyDataPackageNo1304() throws Exception {
        assertPresentInSolrIndex("peggym.127.1");

        SolrDocument data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.129.1");
        assertNotPresentInSolrIndex("peggym.130.4");
        assertPresentInSolrIndex("peggym.resourcemap");
    }

    private void deleteAll() {
        HTTPService httpService = (HTTPService) context.getBean("httpService");
        httpService.sendSolrDelete("peggym.130.4");
        httpService.sendSolrDelete("peggym.127.1");
        httpService.sendSolrDelete("peggym.128.1");
        httpService.sendSolrDelete("peggym.129.1");
        httpService.sendSolrDelete("peggym.resourcemap");
    }

    private void verifyDataPackageNo1271() throws Exception {
        assertNotPresentInSolrIndex("peggym.127.1");
        SolrDocument data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.129.1");

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(1,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(2, documentsCollection.size());
        Assert.assertFalse(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        assertPresentInSolrIndex("peggym.resourcemap");
    }

    private void indexTestDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
    }

    private void indexTestDataPackageWithArchived1271Doc() {
        addSystemMetadata(peggym1271SysArchived);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
    }

    private void verifyTestDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
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
        Assert.assertEquals("peggym.resourcemap",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(3, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        assertPresentInSolrIndex("peggym.resourcemap");
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
        sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);
        sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);
        objectPaths.putAsync(sysmeta.getIdentifier(), path);
        generator.processSystemMetaDataUpdate(sysmeta, path);
    }

    public void setUp() throws Exception {
        super.setUp();
        configureSpringResources();
        configureHazelCast();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        Hazelcast.shutdownAll();
    }

    private void configureSpringResources() {
        processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
        generator = (IndexTaskGenerator) context.getBean("indexTaskGenerator");

        peggym1271Sys = (Resource) context.getBean("peggym1271Sys");
        peggym1271SysArchived = (Resource) context.getBean("peggym1271SysArchived");
        peggym1281Sys = (Resource) context.getBean("peggym1281Sys");
        peggym1291Sys = (Resource) context.getBean("peggym1291Sys");
        peggym1304Sys = (Resource) context.getBean("peggym1304Sys");
        peggym1304SysArchived = (Resource) context.getBean("peggym1304SysArchived");
        peggymResourcemapSys = (Resource) context.getBean("peggymResourcemapSys");
        peggymResourcemapSysArchived = (Resource) context.getBean("peggymResourcemapSysArchived");
    }

    private void configureHazelCast() {
        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.newHazelcastInstance(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        objectPaths = hzMember.getMap(objectPathName);
    }

}
