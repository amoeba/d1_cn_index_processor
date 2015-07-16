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
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
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
    private static HazelcastInstance hzMember;
    private static IMap<Identifier, SystemMetadata> sysMetaMap;
    private static IMap<Identifier, String> objectPaths;

    private IndexTaskProcessor processor;
    private IndexTaskGenerator generator;

    private Resource peggym1271Sys;
    private Resource peggym1271SysArchived;
    private Resource peggym1281Sys;
    private Resource peggym1291Sys;
    private Resource peggym1304Sys;
    private Resource peggym1304SysArchived;
    private Resource peggymResourcemapSys;
    private Resource peggymResourcemap2Sys;
    private Resource peggymResourcemapComplicatedSys;
    private Resource peggymResourcemap2ComplicatedSys;
    private Resource peggymResourcemapSysArchived;
    private Resource peggymResourcemap2SysArchived;
    private Resource peggymResourcemap1OverlapSys;
    private Resource peggymResourcemap2OverlapSys;

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
    public void testArchiveSingleDocFromIndex() throws Exception {
        String pid = "peggym.130.4";
        deleteAll();
        addSystemMetadata(peggym1304Sys);
        processor.processIndexTaskQueue();
        assertPresentInSolrIndex(pid);
        addSystemMetadata(peggym1304SysArchived);
        processor.processIndexTaskQueue();
        assertPresentInSolrIndex(pid);
    }

    @Test
    public void testDeleteSingleDocFromIndex() throws Exception {
        String pid = "peggym.130.4";
        deleteAll();
        addSystemMetadata(peggym1304Sys);
        processor.processIndexTaskQueue();
        assertPresentInSolrIndex(pid);
        deleteSystemMetadata(peggym1304Sys);
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
        verifyDataPackageArchivedResourceMap();
        // update package object (resource map)
        addSystemMetadata(peggym1304Sys);
        processor.processIndexTaskQueue();
        // verify again
        verifyDataPackageArchivedResourceMap();
    }

    /**
     * Test to delete a data package by the removing event.
     */
    @Test
    public void testDeleteDataPackage() throws Exception {
        // create/index data package
        deleteAll();
        indexTestDataPackage();
        //verify in index correct
        verifyTestDataPackageIndexed();
        deleteSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNoResourceMap();
        assertNotPresentInSolrIndex("peggym.resourcemap");
    }

    /**
     * Test to delete a data package while there is another package specifies
     * the same relationship
     */
    @Test
    public void testDeleteDataPackageWithDuplicatedRelationship() throws Exception {
        // create/index data package
        deleteAll();
        indexTestDataPackage();
        //verify in index correct
        verifyTestDataPackageIndexed();
        indexSecondTestDataPackage();
        verifySecondTestDataPackageIndexed();
        deleteSystemMetadata(peggymResourcemap2Sys);
        processor.processIndexTaskQueue();
        // verify data package info correct in index.
        // we removed the second one. So it will recover 
        // to status that only has one resource map
        verifyTestDataPackageIndexed();
        assertNotPresentInSolrIndex("peggym.resourcemap2");
        deleteSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNoResourceMap();
        assertNotPresentInSolrIndex("peggym.resourcemap");
    }

    /**
     * Test to delete data packages having complicated relationship.
     * DataPackage1 - peggym.resourcemap-complicated - describe the following relationship:
     *  peggym.130.4 documents peggym.128.1
     *  peggym.130.4 documents peggym.129.1
     *  peggym.127.1 documents peggym.130.4
     *  So peggym.130.4 is both metadata and data.
     * DataPackage2 - peggym.resourcemap2-complicated - describe the same relationship.
     * 
     */
    @Test
    public void testDeleteDataPackagesWithComplicatedRelation() throws Exception {
        deleteAll();
        indexComplicatedDataPackage();
        verifyComplicatedDataPackageIndexed();
        indexSecondComplicatedDataPackage();
        verifySecondComplicatedDataPackageIndexed();
        deleteSystemMetadata(peggymResourcemap2ComplicatedSys);
        processor.processIndexTaskQueue();
        // verify data package info correct in index.
        // we removed the second one. So it will recover 
        // to status that only has one resource map
        verifyComplicatedDataPackageIndexed();
        assertNotPresentInSolrIndex("peggym.resourcemap2-complicated");
        deleteSystemMetadata(peggymResourcemapComplicatedSys);
        processor.processIndexTaskQueue();
        // verify data package info correct in index
        verifyDataPackageNoResourceMap();
        assertNotPresentInSolrIndex("peggym.resourcemap-complicated");
    }

    /**
     * Two data packages:
     * The first one - peggym.resourcemap1-overlap: peggym.130.4 documents peggym.127.1
     * The second one - peggym.resourcemap2-overlap: peggym.130.4 documents peggym.128.1 and peggym.129.1. 
     * @throws Exception
     */
    @Test
    public void testDeleteTwoOverlappedDataPackage() throws Exception {
        deleteAll();
        indexFirstOverlapDataPackage();
        verifyFirstOverlapDataPackageIndexed();
        indexSecondOverlapDataPackage();
        verifySecondOverlapDataPackageIndexed();
        deleteSystemMetadata(peggymResourcemap2OverlapSys);
        processor.processIndexTaskQueue();
        verifyFirstOverlapDataPackageIndexed();
        assertNotPresentInSolrIndex("peggym.resourcemap2-overlap");
        deleteSystemMetadata(peggymResourcemap1OverlapSys);
        processor.processIndexTaskQueue();
        verifyDataPackageNoResourceMap();
        assertNotPresentInSolrIndex("peggym.resourcemap1-overlap");
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
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.129.1");
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertNotPresentInSolrIndex("peggym.resourcemap");
    }

    private void verifyDataPackageArchivedResourceMap() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.129.1");
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertNotNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP));
        Assert.assertNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNotNull(scienceMetadata.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.resourcemap");
    }

    private void verifyDataPackageNo1304() throws Exception {
        assertPresentInSolrIndex("peggym.127.1");

        SolrDocument data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertNotNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        assertPresentInSolrIndex("peggym.129.1");
        assertPresentInSolrIndex("peggym.130.4");
        assertPresentInSolrIndex("peggym.resourcemap");
    }

    private void deleteAll() {
        HTTPService httpService = (HTTPService) context.getBean("httpService");
        httpService.sendSolrDelete("peggym.130.4");
        httpService.sendSolrDelete("peggym.127.1");
        httpService.sendSolrDelete("peggym.128.1");
        httpService.sendSolrDelete("peggym.129.1");
        httpService.sendSolrDelete("peggym.resourcemap");
        httpService.sendSolrDelete("peggym.resourcemap2");
        httpService.sendSolrDelete("peggym.resourcemap-complicated");
        httpService.sendSolrDelete("peggym.resourcemap2-complicated");
        httpService.sendSolrDelete("peggym.resourcemap1-overlap");
        httpService.sendSolrDelete("peggym.resourcemap2-overlap");
    }

    private void verifyDataPackageNo1271() throws Exception {
        assertPresentInSolrIndex("peggym.127.1");
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
        Assert.assertEquals(3, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        assertPresentInSolrIndex("peggym.resourcemap");
    }

    private void indexFirstOverlapDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemap1OverlapSys);
        processor.processIndexTaskQueue();
    }

    private void indexSecondOverlapDataPackage() {
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemap2OverlapSys);
        processor.processIndexTaskQueue();
    }

    private void indexTestDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemapSys);
        processor.processIndexTaskQueue();
    }

    private void indexSecondTestDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemap2Sys);
        processor.processIndexTaskQueue();
    }

    private void indexComplicatedDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemapComplicatedSys);
        processor.processIndexTaskQueue();
    }

    private void indexSecondComplicatedDataPackage() {
        addSystemMetadata(peggym1271Sys);
        addSystemMetadata(peggym1281Sys);
        addSystemMetadata(peggym1291Sys);
        addSystemMetadata(peggym1304Sys);
        addSystemMetadata(peggymResourcemap2ComplicatedSys);
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

    private void verifyFirstOverlapDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap1-overlap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(1,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap1-overlap",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(1, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));

        assertPresentInSolrIndex("peggym.resourcemap1-overlap");
    }

    private void verifySecondOverlapDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap1-overlap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap2-overlap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.129.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap2-overlap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(2,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap1-overlap",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2-overlap",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(1));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(3, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        assertPresentInSolrIndex("peggym.resourcemap1-overlap");
        assertPresentInSolrIndex("peggym.resourcemap2-overlap");
    }

    private void verifyComplicatedDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_DOCUMENTS)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_DOCUMENTS)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));

        data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.129.1");
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(1,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(2, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));
        Assert.assertEquals(1, ((List) scienceMetadata
                .getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.127.1", ((List) scienceMetadata
                .getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        assertPresentInSolrIndex("peggym.resourcemap-complicated");
    }

    private void verifySecondComplicatedDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(2,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_DOCUMENTS)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_DOCUMENTS)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY));

        data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(2,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.129.1");
        Assert.assertEquals(2,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2-complicated",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));
        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));
        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(2,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap-complicated",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2-complicated",
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(2, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));
        Assert.assertEquals(1, ((List) scienceMetadata
                .getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.127.1", ((List) scienceMetadata
                .getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        assertPresentInSolrIndex("peggym.resourcemap-complicated");
        assertPresentInSolrIndex("peggym.resourcemap2-complicated");
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

    private void verifySecondTestDataPackageIndexed() throws Exception {
        SolrDocument data = assertPresentInSolrIndex("peggym.127.1");
        Assert.assertEquals(2,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.128.1");
        Assert.assertEquals(2,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        data = assertPresentInSolrIndex("peggym.129.1");
        Assert.assertEquals(2,
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2",
                ((List) data.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).get(1));

        Assert.assertEquals(1,
                ((List) data.getFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY)).size());
        Assert.assertEquals("peggym.130.4",
                ((List) data.getFieldValue(SolrElementField.FIELD_ISDOCUMENTEDBY)).get(0));

        Assert.assertNull(data.getFieldValues(SolrElementField.FIELD_DOCUMENTS));

        SolrDocument scienceMetadata = assertPresentInSolrIndex("peggym.130.4");
        Assert.assertEquals(2,
                ((List) scienceMetadata.getFieldValues(SolrElementField.FIELD_RESOURCEMAP)).size());
        Assert.assertEquals("peggym.resourcemap",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(0));
        Assert.assertEquals("peggym.resourcemap2",
                ((List) scienceMetadata.getFieldValue(SolrElementField.FIELD_RESOURCEMAP)).get(1));

        Collection documentsCollection = scienceMetadata
                .getFieldValues(SolrElementField.FIELD_DOCUMENTS);
        Assert.assertEquals(3, documentsCollection.size());
        Assert.assertTrue(documentsCollection.contains("peggym.127.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.128.1"));
        Assert.assertTrue(documentsCollection.contains("peggym.129.1"));

        assertPresentInSolrIndex("peggym.resourcemap");
        assertPresentInSolrIndex("peggym.resourcemap2");
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
        //sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);
        objectPaths.putAsync(sysmeta.getIdentifier(), path);
        generator.processSystemMetaDataUpdate(sysmeta, path);
    }

    private void deleteSystemMetadata(Resource systemMetadataResource) {
        SystemMetadata sysmeta = null;
        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    systemMetadataResource.getInputStream());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }
        sysMetaMap.remove(sysmeta.getIdentifier());
        //sysMetaMap.removeAsync(sysmeta.getIdentifier());
        objectPaths.removeAsync(sysmeta.getIdentifier());
        generator.processSystemMetaDataDelete(sysmeta);
    }

    public void setUp() throws Exception {
        super.setUp();
        configureSpringResources();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        Hazelcast.shutdownAll();
        hzMember = null;
        objectPaths = null;
        sysMetaMap = null;
    }

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
        configureHazelCast();
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
        peggymResourcemap2Sys = (Resource) context.getBean("peggymResourcemap2Sys");
        peggymResourcemapSysArchived = (Resource) context.getBean("peggymResourcemapSysArchived");
        peggymResourcemap2SysArchived = (Resource) context.getBean("peggymResourcemap2SysArchived");
        peggymResourcemapComplicatedSys = (Resource) context
                .getBean("peggymResourcemapComplicatedSys");
        peggymResourcemap2ComplicatedSys = (Resource) context
                .getBean("peggymResourcemap2ComplicatedSys");
        peggymResourcemap1OverlapSys = (Resource) context.getBean("peggymResourcemap1OverlapSys");
        peggymResourcemap2OverlapSys = (Resource) context.getBean("peggymResourcemap2OverlapSys");
    }

    private static void configureHazelCast() {
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
