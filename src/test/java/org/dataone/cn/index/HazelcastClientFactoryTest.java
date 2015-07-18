package org.dataone.cn.index;

import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class HazelcastClientFactoryTest {
	
	private static HazelcastInstance hzMember;
    
	public static void startHazelcast() {

        if (HazelcastClientFactory.getStorageClient() == null) {
        	
            System.out.println("Starting Hazelcast");
            
            Config hzConfig = new ClasspathXmlConfig("org/dataone/configuration/hazelcast.xml");

            System.out.println("Hazelcast Group Config:\n" + hzConfig.getGroupConfig());
            System.out.print("Hazelcast Maps: ");
            for (String mapName : hzConfig.getMapConfigs().keySet()) {
                System.out.print(mapName + " ");
            }
            System.out.println();
            hzMember = Hazelcast.newHazelcastInstance(hzConfig);
            System.out.println("Hazelcast member hzMember name: " + hzMember.getName());

        }
    }
	
	@BeforeClass
	public static void setUp() {
		HazelcastClientFactoryTest.startHazelcast();
	}

	@Test
	public void testSystemMetadataMap() {
		
		int size = HazelcastClientFactory.getSystemMetadataMap().size();
		SystemMetadata sysMeta = new SystemMetadata();
		Identifier identifier = new Identifier();
		identifier.setValue("blah");
		sysMeta.setIdentifier(identifier);
		HazelcastClientFactory.getSystemMetadataMap().put(identifier, sysMeta );
		int newSize = HazelcastClientFactory.getSystemMetadataMap().size();
		Assert.assertTrue(size + 1 == newSize);

		HazelcastClientFactory.getSystemMetadataMap().remove(identifier);

	}
}
