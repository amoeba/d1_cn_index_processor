package org.dataone.cn.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.w3c.dom.Document;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-context.xml" })
public class SolrFieldXPathTest {

    private static Logger logger = Logger.getLogger(SolrFieldXPathTest.class.getName());

    @Autowired
    private Resource peggym1304Sys;
    @Autowired
    private Resource peggym1304Sci;

    // solr/rule field name (from spring context fields) mapped to actual value
    // from xml document
    private HashMap<String, String> eml210Expected = new HashMap<String, String>() {
        {
            put("title", "Augrabies falls National Park census data.");
            put("id", "peggym.130.4");
        }
    };

    @Autowired
    private ScienceMetadataDocumentSubprocessor eml210Subprocessor;

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    @Test
    public void testEml210ScienceMetadataFields() throws Exception {

        Integer fieldCount = Integer.valueOf(0);

        Document scienceMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                peggym1304Sci.getInputStream());
        for (SolrField field : eml210Subprocessor.getFieldList()) {
            boolean compared = compareFields(eml210Expected, scienceMetadataDoc, field);
            if (compared) {
                fieldCount++;
            }
        }

        // test system metadata fields in system metadata config match those
        // in solr index document
        Document systemMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                peggym1304Sys.getInputStream());
        for (SolrField field : getXPathDocumentParser().getFields()) {
            boolean compared = compareFields(eml210Expected, systemMetadataDoc, field);
            if (compared) {
                fieldCount++;
            }
        }
        // if field count is off, some field did not get compared that should
        // have.
        Assert.assertEquals(eml210Expected.keySet().size(), fieldCount.intValue());
    }

    private boolean compareFields(HashMap<String, String> expected, Document metadataDoc,
            SolrField fieldToCompare) throws Exception {

        boolean fieldsCompared = false;
        List<SolrElementField> fields = fieldToCompare.getFields(metadataDoc);
        if (fields.isEmpty() == false) {
            SolrElementField docField = fields.get(0);
            if (expected.containsKey(docField.getName())) {
                String expectedValue = expected.get(docField.getName());
                fieldsCompared = true;
                System.out.println("Comparing value for field " + docField.getName());
                if (expectedValue == null) {
                    Assert.assertTrue(docField.getValue() == null || "".equals(docField.getValue()));
                } else {
                    String docValue = docField.getValue();
                    System.out.println("Doc Value:  " + docValue);
                    System.out.println("Expected Value: " + expectedValue);
                    Assert.assertEquals(expectedValue, docField.getValue());
                }
            }
        }
        return fieldsCompared;
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }
}
