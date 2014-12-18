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

package org.dataone.cn.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.XmlDocumentUtility;
import org.dataone.cn.indexer.parser.BaseXPathDocumentSubprocessor;
import org.dataone.cn.indexer.parser.ISolrField;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public abstract class BaseSolrFieldXPathTest {

    protected final String hostname = Settings.getConfiguration().getString("cn.router.hostname");

    @Autowired
    private ArrayList<SolrIndexService> documentParsers;

    @Autowired
    private BaseXPathDocumentSubprocessor systemMetadata200Subprocessor;

    protected void testXPathParsing(ScienceMetadataDocumentSubprocessor docProcessor,
            Resource sysMetadata, Resource sciMetadata, HashMap<String, String> expectedValues,
            String pid) throws Exception {
        Integer fieldCount = Integer.valueOf(0);

        if (sysMetadata != null) {
            Document systemMetadataDoc = XmlDocumentUtility.generateXmlDocument(sysMetadata
                    .getInputStream());
            for (ISolrField field : systemMetadata200Subprocessor.getFieldList()) {
                boolean compared = compareFields(expectedValues, systemMetadataDoc, field, pid);
                if (compared) {
                    fieldCount++;
                }
            }
        }
        Document scienceMetadataDoc = XmlDocumentUtility.generateXmlDocument(sciMetadata
                .getInputStream());
        for (ISolrField field : docProcessor.getFieldList()) {
            boolean compared = compareFields(expectedValues, scienceMetadataDoc, field, pid);
            if (compared) {
                fieldCount++;
            }
        }

        // if field count is off, some field did not get compared that should
        // have.
        Assert.assertEquals(expectedValues.keySet().size(), fieldCount.intValue());
    }

    protected boolean compareFields(HashMap<String, String> expected, Document metadataDoc,
            ISolrField fieldToCompare, String identifier) throws Exception {

        boolean fieldsCompared = false;
        List<SolrElementField> fields = fieldToCompare.getFields(metadataDoc, identifier);
        if (fields.isEmpty() == false) {
            if (fields.size() > 1) {
                ArrayList<String> actualValues = new ArrayList<String>();
                ArrayList<String> expectedValues = new ArrayList<String>();
                String docFieldName = fields.get(0).getName();
                for (SolrElementField docField : fields) {
                    actualValues.add(docField.getValue());
                }
                if (expected.containsKey(docFieldName)) {
                    CollectionUtils.addAll(expectedValues,
                            StringUtils.split(expected.get(docFieldName), "##"));
                    fieldsCompared = true;
                    System.out.println("Compared fields for: " + docFieldName);
                    System.out.println("Expected values: " + expectedValues);
                    System.out.println("Actual values:   " + actualValues);
                    System.out.println("");
                    Assert.assertTrue("Comparing values for field " + docFieldName,
                            CollectionUtils.isEqualCollection(expectedValues, actualValues));
                } else {
                    System.out.println("Expected does not contain field for: " + docFieldName);
                    Assert.fail("Expected does not contain value for field: " + docFieldName);
                }
            } else {
                SolrElementField docField = fields.get(0);
                if (expected.containsKey(docField.getName())) {
                    String expectedValue = expected.get(docField.getName());
                    expectedValue = expectedValue.replace("\n", "");
                    fieldsCompared = true;
                    System.out.println("Comparing value for field " + docField.getName());
                    if (expectedValue == null) {
                        Assert.assertTrue(docField.getValue() == null
                                || "".equals(docField.getValue()));
                    } else {
                        String docValue = docField.getValue();
                        docValue = docValue.replace("\n", "");
                        System.out.println("Doc Value:      " + docValue);
                        System.out.println("Expected Value: " + expectedValue);
                        System.out.println(" ");
                        Assert.assertEquals("Comparing field: " + docField.getName(),
                                expectedValue, docValue);
                    }
                } else {
                    System.out
                            .println("Expected does not contain field for: " + docField.getName());
                    Assert.fail("Expected does not contain value for field: " + docField.getName());
                }
            }
        } else {
            String expectedValue = expected.get(fieldToCompare.getName());
            System.out.println("Comparing value for missing field: " + fieldToCompare.getName());
            System.out.println("Expected Value: " + expectedValue);
            System.out.println(" ");
            Assert.assertEquals("Comparing field " + fieldToCompare.getName(), expectedValue, "");
            fieldsCompared = true;

        }
        return fieldsCompared;
    }

    protected SolrIndexService getXPathDocumentParser() {
        return documentParsers.get(0);
    }

}
