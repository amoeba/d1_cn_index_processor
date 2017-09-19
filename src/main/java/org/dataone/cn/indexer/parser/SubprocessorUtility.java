package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.springframework.beans.factory.annotation.Autowired;

public class SubprocessorUtility {

    private static Logger logger = Logger.getLogger(SubprocessorUtility.class.getName());

    @Autowired
    private D1IndexerSolrClient d1IndexerSolrClient = null;

    @Autowired
    private String solrQueryUri = null;

    public SubprocessorUtility() {
    }

    /**
     * For every field-name in the fieldsToMerge list, adds the corresponding field from the
     * retrieved SolrDoc (index record) to the provided indexDocument, if they don't have
     * the same value.
     * (If values are different, there will be more than one SolrElementFields in returned SolrDoc
     * with the same name, but different values)
     * This routine effectively controls duplicates in multi-valued fields.
     * @param indexDocument
     * @param fieldsToMerge
     * @return
     * @throws IOException
     * @throws EncoderException
     * @throws XPathExpressionException
     */
    public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument, List<String> fieldsToMerge)
            throws IOException, EncoderException, XPathExpressionException {

        logger.debug("about to merge indexed document with new doc to insert for pid: "
                + indexDocument.getIdentifier());
        SolrDoc solrDoc = d1IndexerSolrClient.retrieveDocumentFromSolrServer(indexDocument.getIdentifier(),
                solrQueryUri);
        if (solrDoc != null) {
            logger.debug("found existing doc to merge for pid: " + indexDocument.getIdentifier());
            for (SolrElementField field : solrDoc.getFieldList()) {
                if (fieldsToMerge.contains(field.getName())
                        && !indexDocument.hasFieldWithValue(field.getName(), field.getValue())) 
                {
                    indexDocument.addField(field);
                
                    logger.debug("merging field: " + field.getName() + " with value: "
                            + field.getValue());
                }
            }
        }
        return indexDocument;
    }
}
