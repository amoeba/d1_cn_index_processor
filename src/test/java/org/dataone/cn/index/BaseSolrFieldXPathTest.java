package org.dataone.cn.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public abstract class BaseSolrFieldXPathTest {

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    protected boolean compareFields(HashMap<String, String> expected, Document metadataDoc,
            SolrField fieldToCompare, String identifier) throws Exception {

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
                }
            } else {
                SolrElementField docField = fields.get(0);
                if (expected.containsKey(docField.getName())) {
                    String expectedValue = expected.get(docField.getName());
                    fieldsCompared = true;
                    System.out.println("Comparing value for field " + docField.getName());
                    if (expectedValue == null) {
                        Assert.assertTrue(docField.getValue() == null
                                || "".equals(docField.getValue()));
                    } else {
                        String docValue = docField.getValue();
                        System.out.println("Doc Value:      " + docValue);
                        System.out.println("Expected Value: " + expectedValue);
                        System.out.println(" ");
                        Assert.assertEquals("Comparing field: " + docField.getName(),
                                expectedValue, docField.getValue());
                    }
                } else {
                    System.out
                            .println("Expected does not contain field for: " + docField.getName());
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

    protected XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }

}
