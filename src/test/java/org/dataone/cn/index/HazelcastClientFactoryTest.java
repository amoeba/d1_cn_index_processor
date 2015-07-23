package org.dataone.cn.index;

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastClientFactoryTest {

    private static HazelcastInstance hzMember;
    private static Logger logger = Logger.getLogger(HazelcastClientFactoryTest.class.getName());

    private static void startHazelcast() {

        if (hzMember == null) {

            logger.debug("Starting Hazelcast");

            Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

            logger.debug("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
            logger.debug("Hazelcast Maps: ");
            for (String mapName : hzConfig.getMapConfigs().keySet()) {
                logger.debug(mapName + " ");
            }
            logger.debug("");
            hzMember = Hazelcast.newHazelcastInstance(hzConfig);

            logger.debug("Hazelcast member hzMember name: " + hzMember.getName());

        }
    }

    @BeforeClass
    public static void setUp() {
        Hazelcast.shutdownAll();
        hzMember = null;
        HazelcastClientFactoryTest.startHazelcast();
    }

    @AfterClass
    public static void shutDown() {
        Hazelcast.shutdownAll();
        hzMember = null;
    }

    @Test
    public void testSystemMetadataMap() {

        int size = HazelcastClientFactory.getSystemMetadataMap().size();
        SystemMetadata sysMeta = new SystemMetadata();
        Identifier identifier = new Identifier();
        identifier.setValue("blah");
        sysMeta.setIdentifier(identifier);
        HazelcastClientFactory.getSystemMetadataMap().put(identifier, sysMeta);
        int newSize = HazelcastClientFactory.getSystemMetadataMap().size();
        Assert.assertTrue(size + 1 == newSize);

        HazelcastClientFactory.getSystemMetadataMap().remove(identifier);

    }
}
