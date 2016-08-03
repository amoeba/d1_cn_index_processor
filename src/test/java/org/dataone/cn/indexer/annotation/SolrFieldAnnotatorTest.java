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

package org.dataone.cn.indexer.annotation;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.cn.index.BaseSolrFieldXPathTest;
import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml", "test-context-annotator.xml" })
public class SolrFieldAnnotatorTest extends BaseSolrFieldXPathTest {

    @Autowired
    private Resource annotation1304Sys;
    @Autowired
    private Resource peggym1304Sci;
    @Autowired
    private Resource annotation1304;

    @Autowired
    private AnnotatorSubprocessor annotatorSubprocessor;

    private SolrDateConverter dateConverter = new SolrDateConverter();

    // what are we expecting from the annotation?
    private HashMap<String, String> annotationExpected = new HashMap<String, String>();

    @Before
    public void setUp() throws Exception {
        // annotations should include the superclass[es]
    	annotationExpected
                .put(AnnotatorSubprocessor.FIELD_ANNOTATION, 
                		"http://ecoinformatics.org/oboe/oboe.1.0/oboe-characteristics.owl#Mass" +
                		"||" +
                		"http://ecoinformatics.org/oboe/oboe.1.0/oboe-core.owl#PhysicalCharacteristic" +
                		"||" +
                		"http://ecoinformatics.org/oboe/oboe.1.0/oboe-core.owl#Characteristic" +
                		"||" +
                		"http://www.w3.org/2000/01/rdf-schema#Resource" +
                		
                		"");
        annotationExpected.put(AnnotatorSubprocessor.FIELD_COMMENT, "Original annotation content");

    	// relationships
        annotationExpected.put(AnnotatorSubprocessor.FIELD_ANNOTATED_BY, "annotation.130.4");
       
        // system metadata
        annotationExpected.put(SolrElementField.FIELD_ID, "peggym.130.4");
        
//        annotationExpected.put("formatId", "http://docs.annotatorjs.org/en/v1.2.x/annotation-format.html");
//        annotationExpected.put("formatType", "METADATA");
//        annotationExpected.put("size", "855");
//        annotationExpected.put("checksum", "df89097223d1afe36cad3d1e2bdda4fd");
//        annotationExpected.put("checksumAlgorithm", "MD5");
//        annotationExpected.put("submitter", "CN=Benjamin Leinfelder A515,O=University of Chicago,C=US,DC=cilogon,DC=org");
//        annotationExpected.put("rightsHolder", "CN=Benjamin Leinfelder A515,O=University of Chicago,C=US,DC=cilogon,DC=org");
//        annotationExpected.put("replicationAllowed", "true");
//        annotationExpected.put("numberReplicas", "");
//        annotationExpected.put("preferredReplicationMN", "");
//        annotationExpected.put("blockedReplicationMN", "");
//        annotationExpected.put("dateUploaded", dateConverter.convert("2014-12-03T23:29:20.262152"));
//        annotationExpected.put("dateModified", dateConverter.convert("2014-12-03T23:29:20.262152"));
//        annotationExpected.put("datasource", "urn:node:KNB");
//        annotationExpected.put("authoritativeMN", "urn:node:KNB");
//        annotationExpected.put("replicaMN", "");
//        annotationExpected.put("replicaVerifiedDate", "");
//        annotationExpected.put("readPermission", "public");
//        annotationExpected.put("changePermission", "");
//        annotationExpected.put("isPublic", "true");

        
    }
    
    protected boolean compareFields(HashMap<String, String> expected, InputStream annotation,
            AnnotatorSubprocessor subProcessor, String identifier, String referencedpid) throws Exception {

        Map<String, SolrDoc> docs = new TreeMap<String, SolrDoc>();
        Map<String, SolrDoc> solrDocs = subProcessor.processDocument(identifier, docs, annotation);
        List<SolrElementField> fields = solrDocs.get(referencedpid).getFieldList();
        
        // make sure our expected fields have the expected values
        for (SolrElementField docField : fields) {
            String name = docField.getName();
            String value = docField.getValue();
            
            String expectedValue = expected.get(name);
            if (expectedValue != null) {
				List<String> expectedValues = Arrays.asList(StringUtils.split(expectedValue , "||"));
	            if (expectedValues != null && !expectedValues.isEmpty()) {
	            	System.out.println("Checking value: " + value);
	            	System.out.println("in expected: " + expectedValues);
	            	Assert.assertTrue(expectedValues.contains(value));  
	            }
            }
        }
        		
        return true;
    }

    /**
     * Testing that the annotation is parsed correctly
     * 
     * @throws Exception
     */
    @Test
    public void testAnnotationFields() throws Exception {
        System.out.println("annotation: " + IOUtils.toString(annotation1304.getInputStream()));

        compareFields(annotationExpected, annotation1304.getInputStream(), annotatorSubprocessor, "annotation.130.4", "peggym.130.4");
    }

}
