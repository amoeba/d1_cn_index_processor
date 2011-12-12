package org.dataone.cn.index;

import static org.junit.Assert.fail;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.index.generator.IndexTaskGeneratorDaemon;
import org.dataone.cn.index.processor.IndexTaskProcessorDaemon;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * This test loads the generator and processor daemons which will open their
 * application context configuration to laod the processor/generator. This means
 * that the config found in the main project will be used to run this test.
 * (PostgreSQL) This test also connects to a Solr server for index processing.
 * 
 * @author sroseboo
 * 
 *         This test class is an integration test, not a unit test. It relies
 *         upon the index generator, processor and configuration of solr,
 *         postgres, hazelcast
 * 
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class IndexTaskProcessingIntegrationTest {

    private static Logger logger = Logger.getLogger(IndexTaskProcessingIntegrationTest.class.getName());

    private HazelcastInstance hzMember;
    private IMap<Identifier, SystemMetadata> sysMetaMap;
    private IMap<Identifier, String> objectPaths;

    private static final String systemMetadataMapName = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private static final String objectPathName = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    @Autowired
    private Resource systemMetadataResource1;
    @Autowired
    private Resource systemMetadataResource2;
    @Autowired
    private Resource systemMetadataResource3;
    @Autowired
    private Resource systemMetadataResource4;
    @Autowired
    private Resource systemMetadataResource5;

    @Test
    public void testGenerateAndProcessIndexTasks() {

        // creating these deamon instance from class loader overrides spring
        // config for jpa repository so postgres is assumed/used.
        IndexTaskGeneratorDaemon generatorDaemon = new IndexTaskGeneratorDaemon();
        IndexTaskProcessorDaemon processorDaemon = new IndexTaskProcessorDaemon();

        try {
            generatorDaemon.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        addSystemMetadata(systemMetadataResource1);
        addSystemMetadata(systemMetadataResource2);
        addSystemMetadata(systemMetadataResource3);
        addSystemMetadata(systemMetadataResource4);
        addSystemMetadata(systemMetadataResource5);

        // Starting processor daemon here to avoid waiting for scheduling
        // interval (2 minutes)
        try {
            processorDaemon.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        try {
            // processing time
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }

        try {
            generatorDaemon.stop();
            processorDaemon.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        Assert.assertTrue(true);
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
        sysMetaMap.putAsync(sysmeta.getIdentifier(), sysmeta);
        objectPaths.putAsync(sysmeta.getIdentifier(), path);
    }

    @Before
    public void setUp() throws Exception {

        Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

        System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
        System.out.print("Hazelcast Maps: ");
        for (String mapName : hzConfig.getMapConfigs().keySet()) {
            System.out.print(mapName + " ");
        }
        System.out.println();
        hzMember = Hazelcast.init(hzConfig);
        System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

        sysMetaMap = hzMember.getMap(systemMetadataMapName);
        objectPaths = hzMember.getMap(objectPathName);
    }

    @After
    public void tearDown() throws Exception {
        Hazelcast.shutdownAll();
    }
}
