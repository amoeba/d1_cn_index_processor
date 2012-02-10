package org.dataone.cn.indexer.parser;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/22/11
 * Time: 1:33 PM
 */

/**Subprocessors test xml expression with return type of boolean.  canProcess function returns true subprocessor is run
 * and can add, remove, or manipulate existing fields in document to be indexed and documents associated to the indexed
 * document.
 *
 */
public interface IDocumentSubprocessor {

    /**Determines if subprocessor should be run on document
     * @param
     * @return returns true if subprocessor should be run on document
     * @throws XPathExpressionException
     */
    boolean canProcess(Document doc) throws XPathExpressionException;

    void initExpression(XPath xpath);

    /**Method allows for manipulation of indexed fields that should be added to solr index
     *
     * @param identifier id of original document in Map docs
     * @param docs Documents indexed by identifiers
     * @param doc original XML document
     * @return Updated solr index documents
     */
    Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs, Document doc) throws Exception;
}
