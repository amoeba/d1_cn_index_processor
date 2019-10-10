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
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.dataone.cn.indexer.XmlDocumentUtility;
import org.dataone.cn.indexer.parser.BaseXPathDocumentSubprocessor;
import org.dataone.cn.indexer.parser.ISolrField;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;
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

    protected static final String hostname = Settings.getConfiguration().getString(
            "cn.router.hostname");

    @Autowired
    private BaseXPathDocumentSubprocessor systemMetadata200Subprocessor;

    
    protected void testXPathParsing(ScienceMetadataDocumentSubprocessor docProcessor,
            Resource sysMetadata, Resource sciMetadata, HashMap<String, String> expectedValues,
            String pid) throws Exception {
        Set<String> checkedFields = new HashSet<String>();

        if (sysMetadata != null) {
            Document systemMetadataDoc = XmlDocumentUtility.generateXmlDocument(sysMetadata
                    .getInputStream());
            checkedFields.addAll(compareFields(expectedValues, systemMetadataDoc, systemMetadata200Subprocessor.getFieldList(), pid));
        }
        Document scienceMetadataDoc = XmlDocumentUtility.generateXmlDocument(sciMetadata
                .getInputStream());
        checkedFields.addAll(compareFields(expectedValues, scienceMetadataDoc, docProcessor.getFieldList(), pid));
        
        // if field count is off, some field did not get compared that should have
        Set<String> expectedFieldSet = expectedValues.keySet();
        if (!CollectionUtils.isEqualCollection(expectedFieldSet, checkedFields)) {
            StringBuilder sb = new StringBuilder();
            for (Object expectedField : CollectionUtils.subtract(expectedFieldSet, checkedFields))
                sb.append(expectedField + ", ");
            throw new AssertionError("Expected fields with no matching field in document: " + sb);
        }
    }

    protected Set<String> compareFields(HashMap<String, String> expected, Document metadataDoc,
            List<ISolrField> fieldsToCompare, String identifier) throws Exception {

        HashMap<String,List<String>> actualValuesByFieldName = new HashMap<String, List<String>>();
        Set<String> emptyFields = new HashSet<String>();
        
        // populate actualValuesByFieldName
        for (ISolrField solrField : fieldsToCompare) {
            // if (!expected.containsKey(solrField.getName())) {
            //     System.out.println("WARNING: no expected value for solr field: " + solrField.getName());
            //     continue;
            // }
            
            List<SolrElementField> fields = solrField.getFields(metadataDoc, identifier);
            
            // emptyFields added to actualValuesByFieldName at end, only if not added by another solrField
            if (fields.isEmpty())
                if (solrField.getName() != null)
                    emptyFields.add(solrField.getName());
            
            for (SolrElementField f : fields) {
                if (actualValuesByFieldName.get(f.getName()) == null) {
                    ArrayList<String> values = new ArrayList<String>();
                    values.add(f.getValue());
                    actualValuesByFieldName.put(f.getName(), values);
                } else {
                    actualValuesByFieldName.get(f.getName()).add(f.getValue());
                }
            }
        }
        
        List<String> emptyValList = new ArrayList<String>();
        emptyValList.add("");
        for (String fieldName : emptyFields)
            if (actualValuesByFieldName.get(fieldName) == null)
                actualValuesByFieldName.put(fieldName, emptyValList );
        
        // compare actual against expected
        for (Entry<String,List<String>> fieldEntry : actualValuesByFieldName.entrySet()) {
            String thisFieldName = fieldEntry.getKey();
            
            List<String> expectedValues = new ArrayList<String>();
            List<String> actualValues = fieldEntry.getValue();
            if (actualValues.size() == 1)
                actualValues.set(0, actualValues.get(0).replace("\n", "")); // to match pre-refactor behavior =/
            
            String expectedForField = expected.get(thisFieldName);
            if (expectedForField == null) {
                System.out.println("No expected value for field: " + thisFieldName);
				throw new AssertionError("No expected value for field " + thisFieldName);
				// continue;
            }
            
            if (expectedForField.equals(""))
                expectedValues.add(expectedForField);
            else
                CollectionUtils.addAll(expectedValues, StringUtils.split(expectedForField, "##"));
           
            Assert.assertThat("For field: " + thisFieldName, actualValues, 
 //                   IsIterableContainingInOrder.contains(expectedValues.toArray()));
                      IsIterableContainingInAnyOrder.containsInAnyOrder(expectedValues.toArray()));

        }
     
        return actualValuesByFieldName.keySet();
    }
}
