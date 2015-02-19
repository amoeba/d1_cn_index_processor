/**
 * This work was created" by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2015
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

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.index.BaseSolrFieldXPathTest;
import org.dataone.cn.indexer.annotation.RdfXmlSubprocessor;
import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * RDF/XML Subprocessor test for provenance field handling
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml", "test-context-provenance.xml" })
public class ProvRdfXmlProcessorTest extends BaseSolrFieldXPathTest {
	
	/* Log it */
	private static Log log = LogFactory.getLog(ProvRdfXmlProcessorTest.class);
	
	/* the RDF/XML resource map to parse */
	@Autowired
	private Resource provAlaWaiNS02MatlabProcessing2RDF;
	
	/* An instance of the RDF/XML Subprocessor */
	@Autowired
	private RdfXmlSubprocessor provRdfXmlSubprocessor;
	
	/* A date converter for many date strings */
	private SolrDateConverter dateConverter = new SolrDateConverter();
	
	/* Store a map of expected Solr fields and their values for testing */
	private HashMap<String, String> expectedFields = new HashMap<String, String>();
	
	/* Define provenance field names used in the index */
    private String WAS_DERIVED_FROM_FIELD          = "prov_wasDerivedFrom";        
    private String WAS_GENERATED_BY_FIELD          = "prov_wasGeneratedBy";         
    private String WAS_INFORMED_BY_FIELD           = "prov_wasInformedBy";          
    private String USED_FIELD                      = "prov_used";                   
    private String GENERATED_BY_PROGRAM_FIELD      = "prov_generatedByProgram";     
    private String GENERATED_BY_EXECUTION_FIELD    = "prov_generatedByExecution";   
    private String GENERATED_BY_USER_FIELD         = "prov_generatedByUser";        
    private String USED_BY_PROGRAM_FIELD           = "prov_usedByProgram";          
    private String USED_BY_EXECUTION_FIELD         = "prov_usedByExecution";        
    private String USED_BY_USER_FIELD              = "prov_usedByUser";             
    private String WAS_EXECUTED_BY_EXECUTION_FIELD = "prov_wasExecutedByExecution"; 
    private String WAS_EXECUTED_BY_USER_FIELD      = "prov_wasExecutedByUser";      
    private String HAS_SOURCES_FIELD               = "prov_hasSources";             
    private String HAS_DERIVATIONS_FIELD           = "prov_hasDerivations";         
    private String INSTANCE_OF_CLASS_FIELD         = "prov_instanceOfClass";        
    
    /**
     * Set up the test data
     * 
     * @throws Exception
     */
    @Before
	public void setUp() throws Exception {
		
    	// For data output object pid
    	expectedFields.put(WAS_DERIVED_FROM_FIELD, "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-ctd-data.1.txt");
    	
    	// For data output object pids
    	expectedFields.put(WAS_GENERATED_BY_FIELD, "urn:uuid:6EC8CAB7-2063-4440-BA23-364313C145FC");
    	
    	// Not added to the test resource map yet
    	//expectedFields.put(WAS_INFORMED_BY_FIELD, "");
    	
    	expectedFields.put(USED_FIELD, "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-ctd-data.1.txt");
    	
    	// For data output object pids
    	expectedFields.put(GENERATED_BY_PROGRAM_FIELD, 
    			"https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-matlab-processing-schedule_AW02XX_001CTDXXXXR00_processing.1.m" +
    			"||" +
    			"https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-matlab-processing-Configure.1.m" +
    			"||" +
    			"https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-matlab-processing-DataProcessor.1.m");
    	expectedFields.put(GENERATED_BY_EXECUTION_FIELD, "urn:uuid:6EC8CAB7-2063-4440-BA23-364313C145FC");
    	expectedFields.put(GENERATED_BY_USER_FIELD, "urn:uuid:D89221AD-E251-4CCB-B515-09D869DB1A61");
    	
    	// For data input object pids
    	expectedFields.put(USED_BY_PROGRAM_FIELD          , "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-matlab-processing-schedule_AW02XX_001CTDXXXXR00_processing.1.m");
    	expectedFields.put(USED_BY_PROGRAM_FIELD          , "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-matlab-processing-Configure.1.m");
    	expectedFields.put(USED_BY_PROGRAM_FIELD          , "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-matlab-processing-DataProcessor.1.m");
    	expectedFields.put(USED_BY_EXECUTION_FIELD        , "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-ctd-data.1.txt");
    	
    	expectedFields.put(USED_BY_USER_FIELD             , "urn:uuid:D89221AD-E251-4CCB-B515-09D869DB1A61");
    	expectedFields.put(WAS_EXECUTED_BY_EXECUTION_FIELD, "urn:uuid:6EC8CAB7-2063-4440-BA23-364313C145FC");
    	expectedFields.put(WAS_EXECUTED_BY_USER_FIELD     , "urn:uuid:D89221AD-E251-4CCB-B515-09D869DB1A61");
    	
    	// For metadata object pids
    	expectedFields.put(HAS_SOURCES_FIELD              , "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-ctd-data.1.txt");
    	
    	// For metadata object pids 
    	expectedFields.put(HAS_DERIVATIONS_FIELD          , "https://cn-sandbox-2.test.dataone.org/cn/v1/resolve/ala-wai-canal-ns02-image-data-AW02XX_001CTDXXXXR00_20150203_10day.1.jpg");
    	
    	// For data input object pids
    	expectedFields.put(INSTANCE_OF_CLASS_FIELD        , "http://www.openarchives.org/ore/terms/ResourceMap");
	}
    
    /* 
     * Compare the indexed provenance Solr fields to the expected fields
     */
    protected boolean compareFields(HashMap<String, String> expectedFields, InputStream resourceMap,
    		RdfXmlSubprocessor provRdfXmlSubProcessor, String identifier, String referencedPid)
    		throws Exception {
		
    	// A map for the sub processor to populate
    	Map<String, SolrDoc> docs = new TreeMap<String, SolrDoc>();
    	docs.put(identifier, new SolrDoc());
    	
    	// The returned map with processed Solr documents
    	Map<String, SolrDoc> solrDocs = provRdfXmlSubProcessor.processDocument(identifier, docs, resourceMap);
    	
    	// A list of Solr fields filtered by the target object identifier
    	List<SolrElementField> fields = solrDocs.get(referencedPid).getFieldList();
    	
    	// compare the expected and indexed fields
    	for (SolrElementField field : fields) {
    		String name = field.getName();
    		String value = field.getValue();
    		
    		String expectedValue = expectedFields.get(name);
    		
    		if (expectedValue != null) {
    			List<String> expectedValues = Arrays.asList(StringUtils.split(expectedValue, "||"));
    			if ( expectedValues != null && !expectedValues.isEmpty() ) {
    				log.debug("Checking value: " + value);
    				log.debug("in expected: " + expectedValues);
    				Assert.assertTrue(expectedValues.contains(value));
    			}
    		}    	
    	
    	}
    	return true;
    	
    }
    
    /**
     * Test if the provenance fields in resource maps are indexed correctly
     * 
     * @throws Exception
     */
    @Ignore
    @Test
    public void testProvenanceFields() throws Exception {
    	
    	log.debug("Testing RDF/XML provenance indexing of ala-wai-ns02-matlab-processing.2.rdf: ");

    	// Ensure fields associated with the resource map (and execution) are indexed
        compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
            "ala-wai-ns02-matlab-processing.2.rdf");    		
    	// Ensure fields associated with the data input objects are indexed
    	compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
            "ala-wai-canal-ns02-ctd-data.1.txt");
    	
    	// Ensure fields associated with the data output objects are indexed
    	compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf",
            "ala-wai-canal-ns02-image-data-AW02XX_001CTDXXXXR00_20150203_10day.1.jpg");

    	// Ensure fields associated with the data input object's metadata are indexed
    	compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
            "ala-wai-canal-ns02-ctd-data.eml.1.xml");
    	
    	// Ensure fields associated with the data output object's metadata are indexed
    	compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
            "ala-wai-canal-ns02-image-data.eml.1.xml");

    }
    
    /**
     *  Default test
     */
    @Test
    public void equalsTest() {
    	Assert.assertTrue(1 == 1);
    	
    }
}
