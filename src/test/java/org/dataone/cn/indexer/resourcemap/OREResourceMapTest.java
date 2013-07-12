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
package org.dataone.cn.indexer.resourcemap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.IOUtils;
import org.dataone.cn.hazelcast.ClientConfiguration;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.cluster.AddOrRemoveConnection;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.logging.Logger;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class OREResourceMapTest
{
	/* Static variables */
	private static int ports[] = { 5740, };
	private static List<String> allMembers = 
			Arrays.asList(new String[]{ String.format("127.0.0.1:%d", ports[0]), });
	
	
	/* Instance variables */
	private ClassPathResource document;
	private HazelcastInstance hazelcast;
	private HazelcastClient client;
	
	@BeforeClass
    public static void init() throws Exception 
    {
        Hazelcast.shutdownAll();
    }
	
	@Before
	public void setupHazelcast() throws FileNotFoundException
	{
		/* HazelCast Server configuration */
		Config hazelcastServerConfig = new Config();
        hazelcastServerConfig.getGroupConfig().setName("DataONEBuildTest").setPassword("passwordTestDataone");
        hazelcastServerConfig.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        hazelcastServerConfig.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        hazelcastServerConfig.setProperty(GroupProperties.PROP_MAX_NO_HEARTBEAT_SECONDS, "10");
        hazelcastServerConfig.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "2");
        hazelcastServerConfig.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "10");
        hazelcastServerConfig.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "10");
        
        NetworkConfig networkConfig = hazelcastServerConfig.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.setPortAutoIncrement(false);
		
        
        hazelcastServerConfig.getNetworkConfig().setPort(ports[0]);
        hazelcastServerConfig.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        
        
        /* Create hazelcast server */
        hazelcast = Hazelcast.newHazelcastInstance(hazelcastServerConfig); 
        
        
        /* Hazelcast client configuration */
        /*ClientConfiguration clientConfiguration = new ClientConfiguration("/tmp/hazelcast.xml");
        
        ClientConfig cc = new ClientConfig();
        
        
        cc.getGroupConfig().setName(clientConfiguration.getGroup());
        cc.getGroupConfig().setPassword(clientConfiguration.getPassword());
        cc.addAddress(clientConfiguration.getLocalhost());
        
        this.client = HazelcastClient.newHazelcastClient(cc);*/
	}
	
	@After
	public void cleanup() throws Exception 
	{
		Hazelcast.shutdownAll();
	}
	
	@Test
	public void testOREResourceMap() throws 
		OREException, URISyntaxException, 
		OREParserException, IOException, XPathExpressionException, 
		SAXException, ParserConfigurationException  
	{
		DocumentBuilderFactory domFactory = 
			    DocumentBuilderFactory.newInstance();
		domFactory.setNamespaceAware(true); 
		DocumentBuilder builder = domFactory.newDocumentBuilder();
		Document doc = builder.parse(document.getFile());
	   
	
		
		ResourceMap foresiteResourceMap = new ForesiteResourceMap(
				IOUtils.toString(this.document.getInputStream()));
		
		ResourceMap xpathResourceMap = new XPathResourceMap(doc);
		
		
		
		/*** Checks that top level identifiers match ***/
		Assert.assertEquals("Identifiers do not match", 
				foresiteResourceMap.getIdentifier(), xpathResourceMap.getIdentifier());
		
		
		
		/*** Tests getAllDocumentIDs() ***/
		List<String>foresiteDocs = foresiteResourceMap.getAllDocumentIDs();
		List<String>xpathDocs = xpathResourceMap.getAllDocumentIDs();
		
		/* Check the number of doc ids */
		Assert.assertEquals("Number of documents IDs don't match", 
				foresiteDocs.size(), xpathDocs.size());
		
		Collections.sort(foresiteDocs);
		Collections.sort(xpathDocs);
		
		/* Check the individual elements match */
		for(int i=0; i<foresiteDocs.size(); i++)
		{
			Assert.assertEquals("Document ID at "+i+"don't match", 
				foresiteDocs.get(i), xpathDocs.get(i));
		}
		
		
		
		/*** Tests the getContains method ***/
		List<String> foresiteContains  = new LinkedList<String>(foresiteResourceMap.getContains());
		List<String> xpathContains = new LinkedList<String>(xpathResourceMap.getContains());
		
		Collections.sort(foresiteContains);
		Collections.sort(xpathContains);
		
		/* Checks that the number of resources is the same */
		Assert.assertEquals("Number of mapped references don't match", 
				foresiteContains.size(), xpathContains.size());
		
		/* Check the individual resource ids match */
		for(int i=0; i<foresiteContains.size(); i++)
		{
			Assert.assertEquals("Document ID at "+i+"don't match", 
					foresiteContains.get(i), xpathContains.get(i));
		}
		
		
		/*** Tests getIdentifierFromResource ***/
		
		
		/*** Tests getMappedReferences ***/
		
		/* Builds a sorted list of documents IDs for comparison */
		List<ResourceEntry> foresiteResourceMapDocs = 
				new LinkedList<ResourceEntry>(foresiteResourceMap.getMappedReferences());
		List<ResourceEntry> xpathResourceMapDocs = 
				new LinkedList<ResourceEntry>(xpathResourceMap.getMappedReferences());
		
		Collections.sort(foresiteResourceMapDocs, new Comparator<ResourceEntry>() 
			{
				@Override
				public int compare(ResourceEntry o1, ResourceEntry o2) 
				{
					return o1.getIdentifier().compareTo(o2.getIdentifier());
				}
			});
		
		Collections.sort(xpathResourceMapDocs, new Comparator<ResourceEntry>() 
			{
				@Override
				public int compare(ResourceEntry o1, ResourceEntry o2) 
				{
					return o1.getIdentifier().compareTo(o2.getIdentifier());
				}
			});
		
		/* Checks that the number of mapped references is the same */
		Assert.assertEquals("Number of mapped references don't match", 
				foresiteResourceMapDocs.size(), xpathResourceMapDocs.size());
				
		/* Check the individual mapped references match */
		for(int i=0; i<foresiteResourceMapDocs.size(); i++)
		{
			Assert.assertEquals("Document ID at "+i+"don't match", 
					foresiteDocs.get(i), xpathDocs.get(i));
		}
    }
	
	@Autowired
	public void setDocument(ClassPathResource document) throws IOException
	{
		this.document = document;
	}
}
