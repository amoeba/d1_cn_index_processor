package org.dataone.cn.index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.convert.SolrDateConverter;
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

    @Autowired
    private Resource peggym1304Sys;
    @Autowired
    private Resource peggym1304Sci;

    @Autowired
    private ScienceMetadataDocumentSubprocessor eml210Subprocessor;

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    SolrDateConverter dateConverter = new SolrDateConverter();

    // solr/rule field name (from spring context fields) mapped to actual value
    // from xml documents (system and science metadata docs)
    private HashMap<String, String> eml210Expected = new HashMap<String, String>() {
        {
            // science metadata
            put("abstract", "");
            put("keywords",
                    "SANParks, South Africa##Augrabies Falls National Park,South Africa##Census data");
            put("title", "Augrabies falls National Park census data.");
            put("southBoundCoord", "1.1875");
            put("northBoundCoord", "1.1875");
            put("westBoundCoord", "26.0");
            put("eastBoundCoord", "26.0");
            put("beginDate", dateConverter.convert("1998"));
            put("endDate", dateConverter.convert("2004-02-13"));
            put("author", "SANParks ");
            put("author_lname", "SANParks");
            put("contactOrganization", "SANParks");

            // system metadata
            put("id", "peggym.130.4");
            put("objectformat", "eml://ecoinformatics.org/eml-2.1.0");
            put("size", "36281");
            put("checksum", "24426711d5385a9ffa583a13d07af2502884932f");
            put("checksumAlgorithm", "SHA-1");
            put("submitter", "dataone_integration_test_user");
            put("rightsholder", "dataone_integration_test_user");
            put("rep_allowed", "true");
            put("n_replicas", "");
            put("pref_rep_mn", "");
            put("blocked_rep_mn", "");
            put("obsoletes", "");
            put("dateuploaded", dateConverter.convert("2011-08-31T15:59:50.071163"));
            put("datemodified", dateConverter.convert("2011-08-31T15:59:50.072921"));
            put("datasource", "test_documents");
            put("auth_mn", "test_documents");
            put("replica_mn", "");
            put("replica_verified", "");
            put("readPermission", "dataone_test_user##dataone_public_user");
            put("writePermission", "dataone_integration_test_user");
            put("changePermission", "");
            put("isPublic", "");
            put("web_url", "");
            put("data_url", "");
        }
    };

    /**
     * Testing that the Xpath expressions used by XPathParser and associates are
     * 'mining' the expected data from the science and system metadata
     * documents.
     * 
     * @throws Exception
     */
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

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }
}
