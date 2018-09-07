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
    
    private boolean usePartialUpdate = false;

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

        if (usePartialUpdate)
                return diffWithIndexedDocument(indexDocument);
        
        logger.debug("about to merge indexed document with new doc to insert for pid: "
                + indexDocument.getIdentifier());
        SolrDoc solrDocFromSolr = d1IndexerSolrClient.retrieveDocumentFromSolrServer(indexDocument.getIdentifier(),
                solrQueryUri);


        if (solrDocFromSolr != null) {
            logger.debug("found existing doc to merge for pid: " + indexDocument.getIdentifier());

            // for all of the fields in specified fieldsToMerge list...
            for (SolrElementField field : solrDocFromSolr.getFieldList()) {
                
                // add the value if it isn't already there (avoids duplicates)
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
    
   
    
    /**
     * Diff the new field values with existing ones in Solr, and return only the fields that are different
     * (to be used for atomic updates)
     * If Solr doesn't contain a record, return the original newIndexDocument, with _version_ = -1 (which allows update only if still new)
     * 
     * @param newIndexDocument - the new fields to potentially add
     * @return a SolrDoc with the fields that are different
     * @throws IOException
     * @throws EncoderException
     * @throws XPathExpressionException
     */
    public SolrDoc diffWithIndexedDocument(SolrDoc newIndexDocument)
            throws IOException, EncoderException, XPathExpressionException {

        logger.debug("about to diff indexed document with new doc to insert for pid: "+ newIndexDocument.getIdentifier());
        logger.debug("...  new doc has " + newIndexDocument.getFieldList().size() + " fields to diff...");
        SolrDoc solrDocFromSolr = 
                d1IndexerSolrClient.retrieveDocumentFromSolrServer(newIndexDocument.getIdentifier(), solrQueryUri);
 
        return diffWithIndexedDocument(newIndexDocument, solrDocFromSolr);
    } 
    
    
        
    public SolrDoc diffWithIndexedDocument(SolrDoc newIndexDocument, SolrDoc oldIndexDocument) {    
        
        if (oldIndexDocument != null) 
        { 
            logger.debug("found existing doc to diff for pid: " + newIndexDocument.getIdentifier());
           
            SolrDoc diffDoc = new SolrDoc(); 
            for (SolrElementField field : newIndexDocument.getFieldList()) 
            {   
                if (field.getName().equals("id") || !oldIndexDocument.hasFieldWithValue(field.getName(), field.getValue()))
                {
                    diffDoc.addField(field);
                    
                    logger.debug("diffing field: " + field.getName() + " with value: " + field.getValue());
                }
            }
            if (oldIndexDocument.getField("_version_") != null) {
                SolrElementField versionField = new SolrElementField();
                versionField.setName("_version_");
                versionField.setValue(oldIndexDocument.getFirstFieldValue(("_version_")));
                diffDoc.addField(versionField);
            }
            logger.debug("...  diff doc has " + diffDoc.getFieldList().size() + " remaining fields");
            return diffDoc;
        } 
        else 
        {
            SolrElementField versionField = new SolrElementField();
            versionField.setName("_version_");
            versionField.setValue("-1");  // this value ensures that update will only happen if the document is new
            newIndexDocument.addField(versionField);
            logger.debug("....  diff doc has " + newIndexDocument.getFieldList().size() + " remaining fields");
            return newIndexDocument;
            
        }
    }
}
