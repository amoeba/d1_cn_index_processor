package org.dataone.cn.indexer;

import java.io.IOException;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.http.client.HttpClient;
import org.dataone.cn.indexer.solrhttp.SolrDoc;

public interface D1IndexerSolrClient {

    /**
     * Posts document data to Solr indexer.
     * 
     * @param uri
     *            Solr index url example:
     *            http://localhost:8080/solr/update?commit=true
     * @param data
     *            documents to index
     * @param encoding
     *            use "UTF-8"
     * @throws IOException
     */

    public void sendUpdate(String uri, List<SolrDoc> data, String encoding)
            throws IOException;

    public void sendUpdate(String uri, List<SolrDoc> data) throws IOException;

    public void sendUpdate(String uri, List<SolrDoc> data, String encoding,
            String contentType) throws IOException;

    public void sendSolrDelete(String pid);

    public void sendSolrDeletes(List<String> pids);

    /**
     * Return the SOLR records for the specified dataone Identifier.  Uses the
     * seriesId and id field to find a match
     * 
     * @param uri
     * @param ids
     * @return
     * @throws IOException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public List<SolrDoc> getDocumentsByD1Identifier(String uri, List<String> ids)
            throws IOException, XPathExpressionException, EncoderException;

    /**
     * gets the solr document(s) where parameter matches the "id" field.
     * @param uri
     * @param id
     * @return
     * @throws IOException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public List<SolrDoc> getDocumentBySolrId(String uri, String id)
            throws IOException, XPathExpressionException, EncoderException;

    /**
     * returns all of documents with the which reference the resourceMapIdentifier
     * @param uri
     * @param resourceMapId
     * @return
     * @throws IOException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public List<SolrDoc> getDocumentsByResourceMap(String uri,
            String resourceMapId) throws IOException, XPathExpressionException,
            EncoderException;

    public List<SolrDoc> getDocumentsByField(String uri,
            List<String> fieldValues, String queryField, boolean maxRows)
            throws IOException, XPathExpressionException, EncoderException;

    public List<SolrDoc> getDocumentsByResourceMapFieldAndDocumentsField(
            String uri, String resourceMapId, String documentsId)
            throws IOException, XPathExpressionException, EncoderException;

    public List<SolrDoc> getDocumentsByResourceMapFieldAndIsDocumentedByField(
            String uri, String resourceMapId, String isDocumentedById)
            throws IOException, XPathExpressionException, EncoderException;

    /**
     * Similar to getDocumentsByD1Identifier, except returns null if none found,
     * and only returns the first document
     * @param id
     * @param solrQueryUri
     * @return the SolrDoc or null if not found
     * @throws XPathExpressionException
     * @throws IOException
     * @throws EncoderException
     */
    public SolrDoc retrieveDocumentFromSolrServer(String id, String solrQueryUri)
            throws XPathExpressionException, IOException, EncoderException;

    public void setSolrSchemaPath(String path);

    public void setSolrIndexUri(String uri);

    public String getSolrIndexUri();

    public HttpClient getHttpClient();

}