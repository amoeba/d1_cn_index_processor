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

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class InvalidXmlCharTest {

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    @Autowired
    private Resource commonBMPCharSetExample;

    @Autowired
    private IndexTaskRepository repo;

    private HazelcastInstance hzMember;
    private IMap<Identifier, SystemMetadata> sysMetaMap;

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static Logger logger = Logger.getLogger(InvalidXmlCharTest.class.getName());

    @Test
    public void testTaskWithBmpCharset() throws Exception {
        SystemMetadata sysmeta = null;
        String pid = "testMNodeTier3:2012679267486_common-bmp-doc-example-ฉันกินกระจกได้";

        repo.deleteInBatch(repo.findByPid(pid));

        try {
            sysmeta = TypeMarshaller.unmarshalTypeFromStream(SystemMetadata.class,
                    commonBMPCharSetExample.getInputStream());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            fail("Test SystemMetadata misconfiguration - Exception " + ex);
        }
        sysMetaMap.put(sysmeta.getIdentifier(), sysmeta);

        IndexTask task = new IndexTask(sysmeta, "");
        InputStream is = new ByteArrayInputStream(task.getSysMetadata().getBytes());
        testXMLParsing(is, pid);

        repo.save(task);
        Thread.sleep(200);
        List<IndexTask> taskList = repo.findByPid(pid);
        Assert.assertEquals(1, taskList.size());

        task = taskList.get(0);
        is = new ByteArrayInputStream(task.getSysMetadata().getBytes());
        testXMLParsing(is, pid);

        Identifier id = new Identifier();
        id.setValue(pid);
        SystemMetadata smd = sysMetaMap.get(id);
        task = new IndexTask(smd, "");
        is = new ByteArrayInputStream(task.getSysMetadata().getBytes());
        testXMLParsing(is, pid);
    }

    private void testXMLParsing(InputStream in, String pid) throws Exception {
        Document sysMeta = getXPathDocumentParser().generateXmlDocument(in);
        System.out.println(" ");
        for (SolrField field : getXPathDocumentParser().getFields()) {
            List<SolrElementField> fields = field.getFields(sysMeta, pid);
            if (fields.isEmpty() == false) {
                for (SolrElementField docField : fields) {
                    System.out.println("field value: " + docField.getValue());
                }
            }
        }
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }

    @Before
    public void setUp() throws Exception {

        if (hzMember == null) {
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
        }
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }
}
