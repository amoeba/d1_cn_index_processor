package org.dataone.cn.indexer.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/22/11
 * Time: 3:24 PM
 */

/**
 * Base functionality for Sub processor.
 * 
 */

public class AbstractDocumentSubprocessor implements IDocumentSubprocessor {

    /**
     * If xpath returns true execute the processDocument Method
     */
    private String matchDocument = null;
    private XPathExpression matchDocumentExpression = null;
    private List<SolrField> fieldList = new ArrayList<SolrField>();

    public String getMatchDocument() {
        return matchDocument;
    }

    public void setMatchDocument(String matchDocument) {
        this.matchDocument = matchDocument;
    }

    /**
     * Default functionality is to process fields like XPathDocumentProcessor
     * and add fields to Solr Document This method maybe overridden to add
     * functionality such as retrieving and updating existing documents in the
     * index.
     * 
     * @param identifier
     *            identifier of System Metadata Document
     * @param docs
     *            Map of Solr Index documents use @identifier to retrieve the
     *            original System Metadata Document
     * @param doc
     *            System Metadata Document
     * @return map of Documents including updated Solr index document
     */
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            Document doc) throws Exception {

        SolrDoc metaDocument = docs.get(identifier);

        for (SolrField solrField : fieldList) {
            try {
                metaDocument.getFieldList().addAll(solrField.getFields(doc, identifier));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return docs;
    }

    /**
     * Returns true if subprocessor should be run against document.
     * 
     * @param doc
     * @return
     * @throws XPathExpressionException
     */
    public boolean canProcess(Document doc) throws XPathExpressionException {

        Boolean matches = (Boolean) matchDocumentExpression.evaluate(doc, XPathConstants.BOOLEAN);
        return matches == null ? false : matches.booleanValue();
    }

    public void initExpression(XPath xpathObject) {
        try {
            matchDocumentExpression = xpathObject.compile(getMatchDocument());
            for (SolrField solrField : fieldList) {
                solrField.initExpression(xpathObject);
            }
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    public List<SolrField> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<SolrField> fieldList) {
        this.fieldList = fieldList;
    }
}
