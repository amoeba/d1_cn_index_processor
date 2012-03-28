package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/26/11
 * Time: 3:51 PM
 */

/**
 * Updates entries for related documents in index. For document relational
 * information refer to
 * http://mule1.dataone.org/ArchitectureDocs-current/design/
 * SearchMetadata.html#id4
 * 
 */
public class ResourceMapSubprocessor extends AbstractDocumentSubprocessor implements
        IDocumentSubprocessor {

    private HTTPService httpService = null;
    private String solrQueryUri = null;

    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            Document doc) throws IOException, EncoderException, SAXException,
            XPathExpressionException, ParserConfigurationException {
        SolrDoc resourceMapDoc = docs.get(identifier);
        List<SolrDoc> processedDocs = processResourceMap(resourceMapDoc, doc);
        Map<String, SolrDoc> processedDocsMap = new HashMap<String, SolrDoc>();
        for (SolrDoc processedDoc : processedDocs) {
            processedDocsMap.put(processedDoc.getIdentifier(), processedDoc);
        }
        return processedDocsMap;
    }

    private List<SolrDoc> processResourceMap(SolrDoc indexDocument, Document resourceMapDocument)
            throws XPathExpressionException, IOException, SAXException,
            ParserConfigurationException, EncoderException {
        ResourceMap resourceMap = new ResourceMap(resourceMapDocument);
        List<String> documentIds = resourceMap.getAllDocumentIDs();
        List<SolrDoc> updateDocuments = getHttpService().getDocuments(getSolrQueryUri(),
                documentIds);
        List<SolrDoc> mergedDocuments = resourceMap.mergeIndexedDocuments(updateDocuments);
        mergedDocuments.add(indexDocument);
        return mergedDocuments;
    }

    public HTTPService getHttpService() {
        return httpService;
    }

    public void setHttpService(HTTPService httpService) {
        this.httpService = httpService;
    }

    public String getSolrQueryUri() {
        return solrQueryUri;
    }

    public void setSolrQueryUri(String solrQueryUri) {
        this.solrQueryUri = solrQueryUri;
    }
}
