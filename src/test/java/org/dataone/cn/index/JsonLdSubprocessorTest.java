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
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.dataone.cn.indexer.parser.JsonLdSubprocessor;
import org.dataone.cn.indexer.resourcemap.RdfXmlProcessorTest;
import org.dataone.service.types.v1.NodeReference;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
    private Resource schemaOrgDoc2;
    private Resource schemaOrgDocSOSO;

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
        schemaOrgDoc2 = (Resource) context.getBean("schemaOrgTestDoc2");
        schemaOrgDocSOSO = (Resource) context.getBean("schemaOrgTestDocSOSO");
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
     * Test the end to end index processing a schema.org 'Dataset' document
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
        formatId = "science-on-schema.org/Dataset;ld+json";
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
        String[] authorLastName = {"Kirby", "Bailey", "Lang", "Brombacher", "Chalk", "Parker",
                "Crocker", "Taylor", "Milton", "Foster", "Raymo", "Kroon", "Bell", "Wilson"};
        assertTrue(compareFieldValue(id, "authorLastName", authorLastName));
        String[] investigator = {"Kirby", "Bailey", "Lang", "Brombacher", "Chalk", "Parker",
                "Crocker", "Taylor", "Milton", "Foster", "Raymo", "Kroon", "Bell", "Wilson"};
        assertTrue(compareFieldValue(id, "investigator", investigator));
        assertTrue(compareFieldValue(id, "awardNumber", new String [] {"http://www.nsf.gov/awardsearch/showAward.do?AwardNumber=1643466"}));
        assertTrue(compareFieldValue(id, "awardTitle", new String [] {"OPP-1643466"}));
        assertTrue(compareFieldValue(id, "pubDate", new String [] {"2020-12-09T00:00:00.000Z"}));
        String[] origins = {"Nicola Kirby", "Ian Bailey", "David C Lang", "A Brombacher", "Thomas B Chalk",
                "Rebecca L Parker", "Anya J Crocker", "Victoria E Taylor", "J Andy Milton", "Gavin L Foster", "Maureen E Raymo", "Dick Kroon", "David B Bell", "Paul A Wilson"};
        assertTrue(compareFieldValue(id, "origin", origins));
        assertTrue(compareFieldValue(id, "funderIdentifier", new String [] {"https://doi.org/10.13039/100000141"}));
        assertTrue(compareFieldValue(id, "funderName", new String [] {"NSF Division of Ocean Sciences"}));
        String[] parts = {"Sub dataset 01", "Sub dataset 02"};
        assertTrue(compareFieldValue(id, "hasPart", parts));
        String[] keywords = {"AMOC", "Atlantic circulation", "B/Ca", "Last Glacial", "MIS 100", "MIS M2", "Nd isotopes"};
        // "box": "-28.09816 -32.95731 41.000022722222 1.71098"
        // i.e. "south west north east" - lat, long of southwest corner ; lat, long of northeast corner
        assertTrue(compareFieldValue(id, "keywords", keywords));
        String [] coord = {"-28.09816"};
        assertTrue(compareFieldValue(id, "southBoundCoord", coord));
        coord[0] = "-32.95731";
        assertTrue(compareFieldValue(id, "westBoundCoord", coord));
        coord[0] = "41.000023";
        assertTrue(compareFieldValue(id, "northBoundCoord", coord));
        coord[0] = "1.71098";
        assertTrue(compareFieldValue(id, "eastBoundCoord", coord));
        assertTrue(compareFieldValue(id, "beginDate", new String [] {"2003-04-21T09:40:00.000Z"}));
        assertTrue(compareFieldValue(id, "endDate", new String [] {"2003-04-26T16:45:00.000Z"}));
        String[] parameters = {"unique record ID number", "Date (UTC) in ISO8601 format: YYYY-MM-DDThh:mmZ",
            "Date (local time zone of PST/PDT) in ISO8601; format: YYYY-MM-DDThh:mm", "Dissolved oxygen"};
        assertTrue(compareFieldValue(id, "parameter", parameters));
        assertTrue(compareFieldValue(id, "edition", "1"));
        assertTrue(compareFieldValue(id, "serviceEndpoint", new String[] {"https://doi.pangaea.de/10.1594/PANGAEA.925562"}));
    }

    /**
     * Test the end to end index processing a schema.org 'Dataset' document. This example
     * contains properties not present or in a different format than the first schema.org example.
     *
     * @throws Exception
     */
    @Test
    public void testInsertSchemaOrg2() throws Exception {
        /* variables used to populate system metadata for each resource */
        File object = null;
        String formatId = null;

        NodeReference nodeid = new NodeReference();
        nodeid.setValue("urn:node:mnTestXXXX");

        String userDN = "uid=tester,o=testers,dc=dataone,dc=org";

        // Insert the schema.org file into the task queue
        String id = "doi.org_10.5061_dryad.m8s2r36";
        formatId = "science-on-schema.org/Dataset;ld+json";
        insertResource(id, formatId, schemaOrgDoc2, nodeid, userDN);

        Thread.sleep(SLEEPTIME);
        // now process the tasks
        processor.processIndexTaskQueue();
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        assertPresentInSolrIndex(id);
        assertTrue(compareFieldValue(id, "title", new String [] {"Context-dependent costs and benefits of a heterospecific nesting association"}));
        assertTrue(compareFieldValue(id, "author", new String [] {"Rose J Swift"}));
        assertTrue(compareFieldValue(id, "authorGivenName", new String [] {"Rose J"}));
        //assertTrue(compareFieldValue(id, "authorLastName", new String [] {"Swift"}));
        String[] origins = {"Rose J Swift", "Amanda D Rodewald", "Nathan R Senner"};
        assertTrue(compareFieldValue(id, "origin", origins));
        String[] keywords = {"Mew Gull", "Larus canus", "Limosa haemastica", "predation", "Hudsonian Godwit",
                "protective nesting association"};
        assertTrue(compareFieldValue(id, "keywords", keywords));
        assertTrue(compareFieldValue(id, "namedLocation", new String [] {"Beluga River", "Alaska"}));
        assertTrue(compareFieldValue(id, "beginDate", new String [] {"2018-03-05T15:54:47.000Z"}));
        assertTrue(compareFieldValue(id, "edition", new String [] {"1"}));
        assertTrue(compareFieldValue(id, "serviceEndpoint", new String[] {"http://datadryad.org/stash/dataset/doi%253A10.5061%252Fdryad.m8s2r36"}));
    }

    /**
     * Test the end to end index processing a schema.org 'Dataset' document. This example
     * contains properties from the ESIP Federation "Science on Schema.org" guidelines full example
     * document.
     *
     * @throws Exception
     */
    @Test
    public void testInsertSchemaOrgSOSO() throws Exception {
        /* variables used to populate system metadata for each resource */
        File object = null;
        String formatId = null;

        NodeReference nodeid = new NodeReference();
        nodeid.setValue("urn:node:mnTestXXXX");
        String userDN = "uid=tester,o=testers,dc=dataone,dc=org";

        // Insert the schema.org file into the task queue
        String id = "doi:10.1234/1234567890";
        formatId = "science-on-schema.org/Dataset;ld+json";
        insertResource(id, formatId, schemaOrgDocSOSO, nodeid, userDN);

        Thread.sleep(SLEEPTIME);
        // now process the tasks
        processor.processIndexTaskQueue();
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        Thread.sleep(SLEEPTIME);
        assertPresentInSolrIndex(id);
        assertTrue(compareFieldValue(id, "prov_wasDerivedFrom", new String[] {"https://doi.org/10.xxxx/Dataset-1"}));
        assertTrue(compareFieldValue(id, "prov_generatedByExecution", new String[] {"https://example.org/executions/execution-42"}));
        assertTrue(compareFieldValue(id, "prov_generatedByProgram", new String[] {"https://somerepository.org/datasets/10.xxxx/Dataset-2.v2/process-script.R"}));
        assertTrue(compareFieldValue(id, "prov_instanceOfClass", new String[] {"http://purl.dataone.org/provone/2015/01/15/ontology#Data"}));
        assertTrue(compareFieldValue(id, "prov_hasDerivations", new String[] {"https://somerepository.org/datasets/10.xxxx/Dataset-101"}));
        assertTrue(compareFieldValue(id, "prov_usedByProgram", new String [] {"https://somerepository.org/datasets/10.xxxx/Dataset-101/process-script.R"}));
        assertTrue(compareFieldValue(id, "prov_usedByExecution", new String [] {"https://example.org/executions/execution-101"}));
    }

    protected boolean compareFieldValue(String id, String fieldName, String[] expectedValues) throws SolrServerException, IOException {
        boolean equal = true;
        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set("q", "id:" + ClientUtils.escapeQueryChars(id));
        solrParams.set("fl", "*");
        QueryResponse qr = getSolrClient().query(solrParams);
        SolrDocument result = qr.getResults().get(0);
        Collection<Object> solrValues = result.getFieldValues(fieldName);
        Object testResult = result.getFirstValue(fieldName);
        String[] solrValuesArray = new String[solrValues.size()];
        if(testResult instanceof Float) {
            // Solr returned a 'Float' value, so convert it to a string so that it can
            // be compared to the expected value.
            System.out.println("++++++++++++++++ Solr returned a 'Float'.");
            int iObj = 0;
            float fval;
            for (Object obj : solrValues) {
               fval = (Float) obj;
               solrValuesArray[iObj] = Float.toString(fval);
               iObj++;
            }
        } else if (testResult instanceof String) {
            solrValuesArray = solrValues.toArray(new String[solrValues.size()]);
        } else if (testResult instanceof Date) {
            // Solr returned a 'Date' value, so convert it to a string so that it can
            // be compared to the expected value.
            System.out.println("++++++++++++++++ Solr returned a 'Date'.");
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            int iObj = 0;

            DateTimeZone.setDefault(DateTimeZone.UTC);
            DateTimeFormatter dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

            Date dateObj;
            DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
            for (Object obj : solrValues) {
                DateTime dateTime = new DateTime(obj);
                solrValuesArray[iObj] = dtfOut.print(dateTime);
                iObj++;
            }
        }

        System.out.println("++++++++++++++++ the solr result array for the field " + fieldName + " is " + solrValuesArray);
        System.out.println("++++++++++++++++ the expected values for the field " + fieldName + " is " + expectedValues);
        if (solrValuesArray.length != expectedValues.length) {
            equal = false;
            return equal;
        }
        if (solrValuesArray.length > 1) {
            Arrays.sort(expectedValues);
            Arrays.sort(solrValuesArray);
        }
        for (int i=0; i<solrValuesArray.length; i++) {
            System.out.println("++++++++++++++++ compare values for field " + "\"" + fieldName + "\"" + " Solr: " + solrValuesArray[i] + " expected value: " + expectedValues[i]);

            if (!solrValuesArray[i].equals(expectedValues[i])) {
                equal = false;
                break;
            }
        }
        return equal;
        
    }
}
