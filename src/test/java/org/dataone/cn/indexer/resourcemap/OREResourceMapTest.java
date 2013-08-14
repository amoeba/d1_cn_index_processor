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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.index.SolrIndexDeleteTest;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;



@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class OREResourceMapTest {

	public class IndexVisibilityDelegateTestImpl implements IndexVisibilityDelegate {

	    public boolean isDocumentVisible(Identifier pid) {
	        return true;
	    }

	    public boolean documentExists(Identifier pid) {
	        return true;
	    }

	}
	
	private static Logger logger = Logger.getLogger(OREResourceMapTest.class.getName());
	
    @Autowired
    private ClassPathResource testDoc;

    @Autowired
    private ClassPathResource incompleteResourceMap;
    
    @Autowired
    private ClassPathResource dryadDoc;
    
    @Autowired
    private ClassPathResource transistiveRelationshipsDoc;
    
    @Autowired
    private ClassPathResource incompleteTransistiveRelationshipsDoc;

    
    @Test
    public void testTransistiveRelationships() throws OREException, URISyntaxException, 
		OREParserException, IOException
    {
    	/* Resource map with all resources visible */
    	ResourceMap resourceMap = new ForesiteResourceMap(
    		IOUtils.toString(transistiveRelationshipsDoc.getInputStream()), 
    		new IndexVisibilityDelegateTestImpl());
    	
    	List<String> docs = resourceMap.getAllDocumentIDs();
    	
    	Assert.assertEquals("Number of documents should be 5", 5, docs.size());
    	
    	
    	
    	Set<ResourceEntry> resources = resourceMap.getMappedReferences();
    	
    	Assert.assertEquals("Number of mapped references should be 4", 4, 
    			resources.size());
    	
    	
    	for(ResourceEntry resource : resources)
    	{
    		logger.info(resource.getIdentifier());
    		
    		if(resource.getIdentifier().equals("resource1"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 0, 
    					documentedBy.size());

    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 1, 
    					documents.size());
    			
    			Assert.assertTrue("Resource1 does not document resource 2", 
    					documents.contains("resource2"));
    			
    	

    		}else if(resource.getIdentifier().equals("resource2"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 1, 
    					documentedBy.size());

    			Assert.assertTrue("Resource2 isn't documented by resource1",
    					documentedBy.contains("resource1"));
    			
    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 1, 
    					documents.size());
    			
    			Assert.assertTrue("Resource2 does not document resource 3", 
    					documents.contains("resource3"));

    		}else if(resource.getIdentifier().equals("resource3"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 1, 
    					documentedBy.size());
    			
    			Assert.assertTrue("Resource3 isn't documented by resource2",
    					documentedBy.contains("resource2"));

    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 1, 
    					documents.size());
    			
    			Assert.assertTrue("Resource3 does not document resource 4", 
    					documents.contains("resource4"));

    		}else if(resource.getIdentifier().equals("resource4"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 1, 
    					documentedBy.size());
    			
    			Assert.assertTrue("Resource4 isn't documented by resource3",
    					documentedBy.contains("resource3"));

    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 0, 
    					documents.size());
    		}
    	}
    }
    
    @Test
    public void testIncompleteTransistiveRelationships() 
    		throws OREException, URISyntaxException, OREParserException, 
    			IOException
    {
    	/* Resource map with all resources visible */
    	ResourceMap resourceMap = new ForesiteResourceMap(
    		IOUtils.toString(incompleteTransistiveRelationshipsDoc.getInputStream()), 
    		new IndexVisibilityDelegateTestImpl());
    	
    	List<String> docs = resourceMap.getAllDocumentIDs();
    	
    	Assert.assertEquals("Number of documents should be 5", 5, docs.size());
    	
    	
    	
    	Set<ResourceEntry> resources = resourceMap.getMappedReferences();
    	
    	Assert.assertEquals("Number of mapped references should be 4", 4, 
    			resources.size());
    	
    	
    	for(ResourceEntry resource : resources)
    	{
    		logger.info(resource.getIdentifier());
    		
    		if(resource.getIdentifier().equals("resource1"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 0, 
    					documentedBy.size());

    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 1, 
    					documents.size());
    			
    			Assert.assertTrue("Resource1 does not document resource 2", 
    					documents.contains("resource2"));
    			
    	

    		}else if(resource.getIdentifier().equals("resource2"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 1, 
    					documentedBy.size());

    			Assert.assertTrue("Resource2 isn't documented by resource1",
    					documentedBy.contains("resource1"));
    			
    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 1, 
    					documents.size());
    			
    			Assert.assertTrue("Resource2 does not document resource 3", 
    					documents.contains("resource3"));

    		}else if(resource.getIdentifier().equals("resource3"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 1, 
    					documentedBy.size());
    			
    			Assert.assertTrue("Resource3 isn't documented by resource2",
    					documentedBy.contains("resource2"));

    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 1, 
    					documents.size());
    			
    			Assert.assertTrue("Resource3 does not document resource 4", 
    					documents.contains("resource4"));

    		}else if(resource.getIdentifier().equals("resource4"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();

    			Assert.assertEquals("Wrong number of documentedBy for "+resource.getIdentifier(), 1, 
    					documentedBy.size());
    			
    			Assert.assertTrue("Resource4 isn't documented by resource3",
    					documentedBy.contains("resource3"));

    			Set<String> documents = resource.getDocuments();

    			Assert.assertEquals("Wrong number of documents for "+resource.getIdentifier(), 0, 
    					documents.size());
    		}
    	}
    }
    
    @Test
    public void testDryadDoc() throws OREException, URISyntaxException, 
    	OREParserException, IOException
    {
    	/* Resource map with all resources visible */
    	ResourceMap resourceMap = new ForesiteResourceMap(
    		IOUtils.toString(dryadDoc.getInputStream()), 
    		new IndexVisibilityDelegateTestImpl());
    	
    	Set<ResourceEntry> resources = resourceMap.getMappedReferences();
    	
    	Assert.assertEquals("Number of mapped references don't match", 
    			13, resources.size());
    	
    	for(ResourceEntry resource : resources)
    	{
    		logger.info(resource.getIdentifier());	
    		
    		if(resource.getIdentifier().equals(
    			"http://dx.doi.org/10.5061/dryad.12&ver=2011-08-02T16:00:05.530-0400"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();
    			
    			Assert.assertEquals("Wrong number of documentedBy", 1, 
    					documentedBy.size());
    			
    			Set<String> documents = resource.getDocuments();
    			
    		}
    		
    		
    		
    		
    	}
    	
    	
    	Assert.fail("Done.");
    }
    
    @Test
    public void testIncompleteResourceMap() throws OREException, 
    	URISyntaxException, OREParserException, IOException
    {
    	/* Resource map with all resources visible */
    	ResourceMap resourceMap = new ForesiteResourceMap(
    		IOUtils.toString(incompleteResourceMap.getInputStream()), 
    		new IndexVisibilityDelegateTestImpl());
    	
    	Set<ResourceEntry> resources = resourceMap.getMappedReferences();
    	
    	Assert.assertEquals("Number of mapped references don't match", 
    			2, resources.size());
    	
    	for(ResourceEntry resource : resources)
    	{
    		logger.info(resource.getIdentifier());	
    		
    		if(resource.getIdentifier().equals("doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.50.1"))
    		{
    			Set<String> documentedBy = resource.getDocumentedBy();
    			
    			Assert.assertEquals("Wrong number of documentedBy", 0, documentedBy.size());
 
    			
    			Set<String> documents = resource.getDocuments();
				
				Assert.assertEquals("Wrong number of documents for doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.50.1", 1, documents.size());
				Assert.assertTrue("doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.50.1 should document doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.40.1", documents.contains("doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.40.1"));
				
				
    			
			}else if(resource.getIdentifier().equals("doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.40.1"))
			{
				Set<String> documentedBy = resource.getDocumentedBy();

				Assert.assertEquals("Wrong number of documentedBy", 1, documentedBy.size());
				Assert.assertTrue("doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.40.1 should be documented by doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.50.1", documentedBy.contains("doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.50.1"));
				
				Set<String> documents = resource.getDocuments();
				
				Assert.assertEquals("Wrong number of documents doi:10.6085/AA/ALEXXX_015MTBD009R00_20110122.40.1", 0, documents.size());
		
			}else
			{
				Assert.fail("Unknown resource id: "+resource.getIdentifier());
			}
    	}
    	
    	Assert.fail("Success");
    }
    
    @Test
    public void testOREResourceMap() throws OREException, URISyntaxException, OREParserException,
            IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        DocumentBuilder builder = domFactory.newDocumentBuilder();
        Document doc = builder.parse(testDoc.getFile());

        ResourceMap foresiteResourceMap = new ForesiteResourceMap(IOUtils.toString(testDoc
                .getInputStream()), new IndexVisibilityDelegateTestImpl());

        ResourceMap xpathResourceMap = new XPathResourceMap(doc,
                new IndexVisibilityDelegateTestImpl());

        /*** Checks that top level identifiers match ***/
        Assert.assertEquals("Identifiers do not match", foresiteResourceMap.getIdentifier(),
                xpathResourceMap.getIdentifier());

        /*** Tests getAllDocumentIDs() ***/
        List<String> foresiteDocs = foresiteResourceMap.getAllDocumentIDs();
        List<String> xpathDocs = xpathResourceMap.getAllDocumentIDs();

        /* Check the number of doc ids */
        Assert.assertEquals("Number of documents IDs don't match", foresiteDocs.size(),
                xpathDocs.size());

        Collections.sort(foresiteDocs);
        Collections.sort(xpathDocs);

        /* Check the individual elements match */
        for (int i = 0; i < foresiteDocs.size(); i++) {
            Assert.assertEquals("Document ID at " + i + "don't match", foresiteDocs.get(i),
                    xpathDocs.get(i));
        }

        /*** Tests the getContains method ***/
        List<String> foresiteContains = new LinkedList<String>(foresiteResourceMap.getContains());
        List<String> xpathContains = new LinkedList<String>(xpathResourceMap.getContains());

        Collections.sort(foresiteContains);
        Collections.sort(xpathContains);

        /* Checks that the number of resources is the same */
        Assert.assertEquals("Number of mapped references don't match", foresiteContains.size(),
                xpathContains.size());

        /* Check the individual resource ids match */
        for (int i = 0; i < foresiteContains.size(); i++) {
            Assert.assertEquals("Document ID at " + i + "don't match", foresiteContains.get(i),
                    xpathContains.get(i));
        }

        /*** Tests getIdentifierFromResource ***/

        /*** Tests getMappedReferences ***/

        /* Builds a sorted list of documents IDs for comparison */
        List<ResourceEntry> foresiteResourceMapDocs = new LinkedList<ResourceEntry>(
                foresiteResourceMap.getMappedReferences());
        List<ResourceEntry> xpathResourceMapDocs = new LinkedList<ResourceEntry>(
                xpathResourceMap.getMappedReferences());

        Collections.sort(foresiteResourceMapDocs, new Comparator<ResourceEntry>() {
            @Override
            public int compare(ResourceEntry o1, ResourceEntry o2) {
                return o1.getIdentifier().compareTo(o2.getIdentifier());
            }
        });

        Collections.sort(xpathResourceMapDocs, new Comparator<ResourceEntry>() {
            @Override
            public int compare(ResourceEntry o1, ResourceEntry o2) {
                return o1.getIdentifier().compareTo(o2.getIdentifier());
            }
        });

        /* Checks that the number of mapped references is the same */
        Assert.assertEquals("Number of mapped references don't match",
                foresiteResourceMapDocs.size(), xpathResourceMapDocs.size());

        /* Check the individual mapped references match */
        for (int i = 0; i < foresiteResourceMapDocs.size(); i++) {
            Assert.assertEquals("Document ID at " + i + "don't match", foresiteDocs.get(i),
                    xpathDocs.get(i));
        }
    }

    /**
     * Test scenario that ensures that pids that do not have system metadata in the 
     * system yet, still appear in the pid list as pids that are referenced by this resource map
     * and need to appear in the search index.
     * 
     * Test uses inner class - NullSmdVisibilityDelegate to simulate pids that do not have system
     * metadata records.
     */
    @Test
    public void testOREParsingWithNullSystemMetadataReferences()
            throws ParserConfigurationException, SAXException, IOException, OREException,
            URISyntaxException, OREParserException, XPathExpressionException {

        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        domFactory.setNamespaceAware(true);
        DocumentBuilder builder = domFactory.newDocumentBuilder();
        Document doc = builder.parse(testDoc.getFile());

        ResourceMap foresiteResourceMap = new ForesiteResourceMap(IOUtils.toString(testDoc
                .getInputStream()), new NullSmdVisibilityDelegate());

        ResourceMap xpathResourceMap = new XPathResourceMap(doc, new NullSmdVisibilityDelegate());

       
        Map<Identifier, Map<Identifier, List<Identifier>>> relations = org.dataone.ore.ResourceMapFactory
                .getInstance().parseResourceMap(testDoc.getInputStream());

        int pidCount = 1;
        Identifier identifier = relations.keySet().iterator().next();
        Map<Identifier, List<Identifier>> identiferMap = (Map<Identifier, List<Identifier>>) relations
                .get(identifier);
        for (Map.Entry<Identifier, List<Identifier>> entry : identiferMap.entrySet()) {
            pidCount++;
            for (Identifier documentedByIdentifier : entry.getValue()) {
                pidCount++;
            }
        }

        System.out.println("pidCount: " + pidCount);
        System.out.println("foresite all ids: " + foresiteResourceMap.getAllDocumentIDs().size());
        System.out.println("xpath all ids: " + xpathResourceMap.getAllDocumentIDs().size());
        Assert.assertEquals("foresite pid count does not match actual pid count.", pidCount,
                foresiteResourceMap.getAllDocumentIDs().size());
        Assert.assertEquals("xpath pid count does not match actual pid count.", pidCount,
                xpathResourceMap.getAllDocumentIDs().size());
    }

    private class NullSmdVisibilityDelegate implements IndexVisibilityDelegate {

        public boolean isDocumentVisible(Identifier pid) {
            return true;
        }

        public boolean documentExists(Identifier pid) {
            return false;
        }
    }

}
