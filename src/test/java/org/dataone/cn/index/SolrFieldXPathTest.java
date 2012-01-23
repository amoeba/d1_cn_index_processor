package org.dataone.cn.index;

import java.net.InetAddress;
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
import org.junit.Before;
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

    private SolrDateConverter dateConverter = new SolrDateConverter();

    // solr/rule field name (from spring context fields) mapped to actual value
    // from xml documents (system and science metadata docs)
    private HashMap<String, String> eml210Expected = new HashMap<String, String>();

    @Before
    public void setUp() throws Exception {
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        // science metadata
        eml210Expected.put("abstract", "");
        eml210Expected.put("keywords",
                "SANParks, South Africa##Augrabies Falls National Park,South Africa##Census data");
        eml210Expected.put("title", "Augrabies falls National Park census data.");
        eml210Expected.put("southBoundCoord", "1.1875");
        eml210Expected.put("northBoundCoord", "1.1875");
        eml210Expected.put("westBoundCoord", "26.0");
        eml210Expected.put("eastBoundCoord", "26.0");
        eml210Expected.put("beginDate", dateConverter.convert("1998"));
        eml210Expected.put("endDate", dateConverter.convert("2004-02-13"));
        eml210Expected.put("author", "SANParks ");
        eml210Expected.put("author_lname", "SANParks");
        eml210Expected.put("contactOrganization", "SANParks");
        eml210Expected.put("fileID", "https://" + hostname + "/cn/v1/resolve/peggym.130.4");

        // system metadata
        eml210Expected.put("id", "peggym.130.4");
        eml210Expected.put("objectformat", "eml://ecoinformatics.org/eml-2.1.0");
        eml210Expected.put("size", "36281");
        eml210Expected.put("checksum", "24426711d5385a9ffa583a13d07af2502884932f");
        eml210Expected.put("checksumAlgorithm", "SHA-1");
        eml210Expected.put("submitter", "dataone_integration_test_user");
        eml210Expected.put("rightsholder", "dataone_integration_test_user");
        eml210Expected.put("rep_allowed", "true");
        eml210Expected.put("n_replicas", "");
        eml210Expected.put("pref_rep_mn", "");
        eml210Expected.put("blocked_rep_mn", "");
        eml210Expected.put("obsoletes", "");
        eml210Expected.put("dateuploaded", dateConverter.convert("2011-08-31T15:59:50.071163"));
        eml210Expected.put("datemodified", dateConverter.convert("2011-08-31T15:59:50.072921"));
        eml210Expected.put("datasource", "test_documents");
        eml210Expected.put("auth_mn", "test_documents");
        eml210Expected.put("replica_mn", "");
        eml210Expected.put("replica_verified", "");
        eml210Expected.put("readPermission", "dataone_test_user##dataone_public_user");
        eml210Expected.put("writePermission", "dataone_integration_test_user");
        eml210Expected.put("changePermission", "");
        eml210Expected.put("isPublic", "");
        eml210Expected.put("web_url", "");
        eml210Expected.put("data_url", "https://" + hostname + "/cn/v1/resolve/peggym.130.4");
    }

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
