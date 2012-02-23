package org.dataone.cn.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrDocument;
import org.dataone.cn.indexer.parser.ScienceMetadataDocumentSubprocessor;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.util.DateTimeMarshaller;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.Resource;
import org.w3c.dom.Document;

/**
 * Solr unit test framework is dependent on JUnit 4.7. Later versions of junit
 * will break the base test classes.
 * 
 * @author sroseboo
 * 
 */
public class SolrIndexFieldTest extends DataONESolrJettyTestBase {

    private static Logger logger = Logger.getLogger(SolrIndexFieldTest.class.getName());

    // TODO: test resource map / data packaging index properties?

    @Test
    public void testSystemMetadataAndEml210ScienceData() throws Exception {
        // peggym.130.4 system metadata document for eml2.1.0 science metadata
        // document
        String pid = "peggym.130.4";
        Resource systemMetadataResource = (Resource) context.getBean("peggym1304Sys");

        // add peggym.130.4 to solr index, using XPathDocumentParser (used by
        // index-task-processor)
        addToSolrIndex(systemMetadataResource);

        // retrieve solrDocument for peggym130.4 from solr server by pid
        SolrDocument result = assertPresentInSolrIndex(pid);

        HTTPService httpService = (HTTPService) context.getBean("httpService");

        SolrDoc solrDoc = httpService.retrieveDocumentFromSolrServer(pid,
                "http://localhost:8983/solr/select/");

        // test science metadata fields in eml210 config match actual fields in
        // solr index document
        ScienceMetadataDocumentSubprocessor eml210 = (ScienceMetadataDocumentSubprocessor) context
                .getBean("eml210Subprocessor");

        Resource scienceMetadataResource = (Resource) context.getBean("peggym1304Sci");
        Document scienceMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                scienceMetadataResource.getInputStream());
        for (SolrField field : eml210.getFieldList()) {
            compareFields(result, scienceMetadataDoc, field, pid);
        }

        // test system metadata fields in system metadata config match those
        // in solr index document
        Document systemMetadataDoc = getXPathDocumentParser().generateSystemMetadataDoc(
                systemMetadataResource.getInputStream());
        for (SolrField field : getXPathDocumentParser().getFields()) {
            compareFields(result, systemMetadataDoc, field, pid);
        }
    }

    private void compareFields(SolrDocument solrResult, Document metadataDoc,
            SolrField fieldToCompare, String identifier) throws Exception {
        List<SolrElementField> fields = fieldToCompare.getFields(metadataDoc, identifier);
        if (fields.isEmpty() == false) {
            SolrElementField docField = fields.get(0);
            Object solrValueObject = solrResult.getFieldValue(docField.getName());

            System.out.println("Comparing value for field " + docField.getName());
            if (solrValueObject == null) {
                if (!"text".equals(docField.getName())) {
                    Assert.assertTrue(docField.getValue() == null || "".equals(docField.getValue()));
                }
            } else if (solrValueObject instanceof String) {
                String solrValue = (String) solrValueObject;
                String docValue = docField.getValue();
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docField.getValue(), solrValue);
            } else if (solrValueObject instanceof Boolean) {
                Boolean solrValue = (Boolean) solrValueObject;
                Boolean docValue = Boolean.valueOf(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue, solrValue);
            } else if (solrValueObject instanceof Long) {
                Long solrValue = (Long) solrValueObject;
                Long docValue = Long.valueOf(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue, solrValue);
            } else if (solrValueObject instanceof Float) {
                Float solrValue = (Float) solrValueObject;
                Float docValue = Float.valueOf(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue, solrValue);
            } else if (solrValueObject instanceof Date) {
                Date solrValue = (Date) solrValueObject;
                Date docValue = DateTimeMarshaller.deserializeDateToUTC(docField.getValue());
                System.out.println("Doc Value:  " + docValue);
                System.out.println("Solr Value: " + solrValue);
                Assert.assertEquals(docValue.getTime(), solrValue.getTime());
            } else if (solrValueObject instanceof ArrayList) {
                ArrayList solrValueArray = (ArrayList) solrValueObject;
                ArrayList documentValueArray = new ArrayList();
                for (SolrElementField sef : fields) {
                    documentValueArray.add(sef.getValue());
                }
                System.out.println("Doc Value:  " + documentValueArray);
                System.out.println("Solr Value: " + solrValueArray);
                Assert.assertTrue(CollectionUtils.isEqualCollection(documentValueArray,
                        solrValueArray));
            } else {
                Assert.assertTrue(
                        "Unknown solr value object type for field: " + docField.getName(), false);
            }
            System.out.println("");
        }
    }

}
