/**
 * This work was crfield name: eated" by participants in the DataONE project, and is
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.index.DataONESolrJettyTestBase;
import org.dataone.cn.indexer.annotation.RdfXmlSubprocessor;
import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;

/**
 * RDF/XML Subprocessor test for provenance field handling
 */
public class ProvRdfXmlProcessorTest extends DataONESolrJettyTestBase {
	
	/* Log it */
	private static Log log = LogFactory.getLog(ProvRdfXmlProcessorTest.class);
	
	/* the conext with provenance-specific bean definitions */
	private ApplicationContext provenanceContext = null;        

	/* the RDF/XML resource map to parse */
	private Resource provAlaWaiNS02MatlabProcessing2RDF;
	
	/* An instance of the RDF/XML Subprocessor */
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
     * Set up the Solr service and test data
     * 
     * @throws Exception
     */
    @Before
	public void setUp() throws Exception {
		
    	// Start up the embedded Jetty server and Solr service
    	super.setUp();
    	
    	// load the prov context beans
    	loadProvenanceContext();
    	
    	// instantiate the subprocessor
    	provRdfXmlSubprocessor = (RdfXmlSubprocessor) context.getBean("provRdfXmlSubprocessor");
    	
    	
	}

    /**
     * Clean up, bring down the Solr service
     */
	@After
    public void tearDown() throws Exception {
    	super.tearDown();
    	
    }
	
    /* 
     * Compare the indexed provenance Solr fields to the expected fields
     */
    protected boolean compareFields(HashMap<String, String> expectedFields, InputStream resourceMap,
    		RdfXmlSubprocessor provRdfXmlSubProcessor, String identifier, String referencedPid)
    		throws Exception {
		
    	// A map for the sub processor to populate
    	Map<String, SolrDoc> docs = new TreeMap<String, SolrDoc>();
    	
    	// Build a minimal SolrDoc keyed by the id
    	SolrDoc solrDoc = new SolrDoc();
    	SolrElementField identifierField = new SolrElementField();
    	identifierField.setName(SolrElementField.FIELD_ID);
    	identifierField.setValue(identifier);
		solrDoc.addField(identifierField);
    	docs.put(identifier, solrDoc);
    	    	
    	// The returned map with processed Solr documents
    	Map<String, SolrDoc> solrDocs = provRdfXmlSubProcessor.processDocument(identifier, docs, resourceMap);
    	
    	// A list of Solr fields filtered by the target object identifier
    	List<SolrElementField> fields = solrDocs.get(referencedPid).getFieldList();
    	
    	// compare the expected and processed fields
    	for (SolrElementField field : fields) {
    		String name = field.getName();
    		String value = field.getValue();
    		log.debug("Field name: " + name);
    		String expectedValue = expectedFields.get(name);
    		
    		if (expectedValue != null) {
    			List<String> expectedValues = Arrays.asList(StringUtils.split(expectedValue, "||"));
    			if ( expectedValues != null && !expectedValues.isEmpty() ) {
    				log.debug("Checking value:\t" + value);
    				log.debug("in expected: \t" + expectedValues);
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
    @Test
    public void testProvenanceFields() throws Exception {
    	
    	log.debug("Testing RDF/XML provenance indexing of ala-wai-ns02-matlab-processing.2.rdf: ");
    	
    	// Ensure fields associated with the data input objects are indexed
    	expectedFields.clear();
    	expectedFields.put(USED_BY_PROGRAM_FIELD, 
    			"ala-wai-canal-ns02-matlab-processing-schedule_AW02XX_001CTDXXXXR00_processing.1.m" + "||" +
    			"ala-wai-canal-ns02-matlab-processing-Configure.1.m" + "||" +
    			"ala-wai-canal-ns02-matlab-processing-DataProcessor.1.m");
    	expectedFields.put(USED_BY_EXECUTION_FIELD, "urn:uuid:6EC8CAB7-2063-4440-BA23-364313C145FC");
    	expectedFields.put(INSTANCE_OF_CLASS_FIELD, "http://purl.org/provone/2015/15/ontology#Data");
    	expectedFields.put(USED_BY_USER_FIELD, "urn:uuid:D89221AD-E251-4CCB-B515-09D869DB1A61");
    	compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
            "ala-wai-canal-ns02-ctd-data.1.txt");
    	
    	// Ensure fields associated with the data output objects are indexed
    	expectedFields.clear();
    	expectedFields.put(WAS_GENERATED_BY_FIELD, "urn:uuid:6EC8CAB7-2063-4440-BA23-364313C145FC");
    	expectedFields.put(WAS_DERIVED_FROM_FIELD, "ala-wai-canal-ns02-ctd-data.1.txt");
    	expectedFields.put(GENERATED_BY_PROGRAM_FIELD, 
    			"ala-wai-canal-ns02-matlab-processing-schedule_AW02XX_001CTDXXXXR00_processing.1.m" + "||" +
    			"ala-wai-canal-ns02-matlab-processing-Configure.1.m" + "||" +
    			"ala-wai-canal-ns02-matlab-processing-DataProcessor.1.m");
    	expectedFields.put(GENERATED_BY_EXECUTION_FIELD, "urn:uuid:6EC8CAB7-2063-4440-BA23-364313C145FC");
    	expectedFields.put(GENERATED_BY_USER_FIELD, "urn:uuid:D89221AD-E251-4CCB-B515-09D869DB1A61");
    	expectedFields.put(INSTANCE_OF_CLASS_FIELD, "http://purl.org/provone/2015/15/ontology#Data");
    	compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
            provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf",
            "ala-wai-canal-ns02-image-data-AW02XX_001CTDXXXXR00_20150203_10day.1.jpg");
        
    	// Ensure fields associated with the data input object's metadata are indexed
    	//expectedFields.clear();
    	//expectedFields.put(HAS_DERIVATIONS_FIELD, "ala-wai-canal-ns02-image-data-AW02XX_001CTDXXXXR00_20150203_10day.1.jpg");
    	//compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
        //    provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
        //    "ala-wai-canal-ns02-ctd-data.eml.1.xml");
    	
    	// Ensure fields associated with the data output object's metadata are indexed
    	//expectedFields.clear();
    	//expectedFields.put(HAS_SOURCES_FIELD, "ala-wai-canal-ns02-ctd-data.1.txt");
    	//compareFields(expectedFields, provAlaWaiNS02MatlabProcessing2RDF.getInputStream(), 
        //    provRdfXmlSubprocessor, "ala-wai-ns02-matlab-processing.2.rdf", 
        //    "ala-wai-canal-ns02-image-data.eml.1.xml");

    }
    
    /**
     *  Default test - is JUnit working as expected?
     */
    @Ignore
    @Test
    public void testInit() {
    	Assert.assertTrue(1 == 1);
    	
    }
    
    /* Load the provence context beans */
    protected void loadProvenanceContext() throws IOException {
        if (provenanceContext == null) {
        	provenanceContext = 
        			new ClassPathXmlApplicationContext(
        				"org/dataone/cn/indexer/resourcemap/test-context-provenance.xml");
        }
        
        provAlaWaiNS02MatlabProcessing2RDF = 
        	(Resource) provenanceContext.getBean("provAlaWaiNS02MatlabProcessing2RDF");
        
    }

}
