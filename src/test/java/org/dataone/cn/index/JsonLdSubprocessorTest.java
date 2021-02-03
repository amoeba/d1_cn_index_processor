/**
 * This work was crfield name: eated" by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2021
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

import java.io.File;

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.indexer.parser.JsonLdSubprocessor;
import org.dataone.cn.indexer.resourcemap.RdfXmlProcessorTest;
import org.dataone.service.types.v1.NodeReference;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.core.io.Resource;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Test the json-ld subprocessor
 * @author tao
 *
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class JsonLdSubprocessorTest extends RdfXmlProcessorTest {
    
    /* Log it */
    private static Log log = LogFactory.getLog(JsonLdSubprocessorTest.class);

    /* The schema.org object */
    private Resource schemaOrgDoc;

    /* An instance of the RDF/XML Subprocessor */
    private JsonLdSubprocessor jsonLdSubprocessor;


    /* Store a map of expected Solr fields and their values for testing */
    private HashMap<String, String> expectedFields = new HashMap<String, String>();

    private static final int SLEEPTIME = 5000;


    /**
     * For each test, set up the Solr service and test data
     * 
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // Start up the embedded Jetty server and Solr service
        super.setUp();
        schemaOrgDoc = (Resource) context.getBean("schemaOrgTestDoc");
        // instantiate the subprocessor
        jsonLdSubprocessor = (JsonLdSubprocessor) context.getBean("jsonLdSubprocessor");

    }


    /**
     * For each test, clean up, bring down the Solr service
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();

    }

    /**
     * Test the end to end index processing of a resource map with provenance statements
     * 
     * @throws Exception
     */
    //@Ignore
    @Test
    public void testInsertSchemaOrg() throws Exception {
        /* variables used to populate system metadata for each resource */
        File object = null;
        String formatId = null;

        NodeReference nodeid = new NodeReference();
        nodeid.setValue("urn:node:mnTestXXXX");

        String userDN = "uid=tester,o=testers,dc=dataone,dc=org";
        
        // Insert the schema.org file into the task queue
        String id = "urn:uuid:f18812ac-7f4f-496c-82cc-3f4f54830289";
        formatId = "science-on-schema.org/Dataset/1.2;ld+json";
        insertResource(id, formatId, schemaOrgDoc, nodeid, userDN);

        Thread.sleep(SLEEPTIME);
        // now process the tasks
        processor.processIndexTaskQueue();
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        assertPresentInSolrIndex(id);
        assertTrue(compareFieldValue(id, "title", "Neodymium isotopes, B/Ca and δ¹³C, and fresh sand volcanic glass count data from ODP Site 208-1267 and IODP Site 306-U1313 for MIS M2, MIS 100 and the Last Glacial-Holocene"));
        assertTrue(compareFieldValue(id, "abstract", "Marine Isotope Stage (MIS) M2, 3.3 Ma, is an isolated cold stage punctuating the benthic oxygen isotope (δ¹⁸O) stratigraphy of the warm Piacenzian interval of the late Pliocene Epoch. The prominent (~0.65‰) δ¹⁸O increase that defines MIS M2 has prompted debate over the extent to which it signals an early prelude to the rhythmic extensive glaciations of the northern hemisphere that characterise the Quaternary and raised questions about the forcing mechanisms responsible. Recent work suggests that CO₂ storage in the deep Atlantic Ocean played an important role in these events but detailed reconstructions of deep ocean chemical stratification are needed to test this idea and competing hypotheses. Here we present new records of the Nd isotope composition of fish debris and δ¹³C and B/Ca ratios of benthic foraminifera from the northwest and southeast Atlantic Ocean. […]"));
        assertTrue(compareFieldValue(id, "label", "Neodymium isotopes"));
        assertTrue(compareFieldValue(id, "author", "Nicola Kirby"));
        assertTrue(compareFieldValue(id, "authorGivenName", "Nicola"));
//        assertTrue(compareFieldValue(id, "authorLastName", "Kirby"));
//        assertTrue(compareFieldValue(id, "orgin", ""));
//        assertTrue(compareFieldValue(id, "hasPart", ""));
//        assertTrue(compareFieldValue(id, "keyword", ""));
//        assertTrue(compareFieldValue(id, "southBoundCoord", "28.09816"));
//        assertTrue(compareFieldValue(id, "westBoundCoord", "32.95731"));
//        assertTrue(compareFieldValue(id, "northBoundCoord", "41.000022722222"));
//        assertTrue(compareFieldValue(id, "eastBoundCoord", "1.71098"));
//        assertTrue(compareFieldValue(id, "namedLocation", ""));
//        assertTrue(compareFieldValue(id, "beginDate", "2003-04-21T09:40:00"));
//        assertTrue(compareFieldValue(id, "endDate", "2003-04-26T16:45:00"));
//        assertTrue(compareFieldValue(id, "paramter", ""));
        assertTrue(compareFieldValue(id, "edition", ""));
//        assertTrue(compareFieldValue(id, "serverEndPoint", ""));

    }
}
