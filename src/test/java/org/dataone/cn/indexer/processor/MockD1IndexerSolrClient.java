package org.dataone.cn.indexer.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.http.client.HttpClient;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.solrhttp.SolrDoc;


public class MockD1IndexerSolrClient implements D1IndexerSolrClient {

    @Override
    public void sendUpdate(String uri, List<SolrDoc> data, String encoding)
            throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendUpdate(String uri, List<SolrDoc> data) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendUpdate(String uri, List<SolrDoc> data, String encoding,
            boolean isPartialUpdate) throws IOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void sendSolrDelete(String pid) {
        // TODO Auto-generated method stub

    }

    @Override
    public void sendSolrDeletes(List<String> pids) {
        // TODO Auto-generated method stub

    }

    @Override
    public List<SolrDoc> getDocumentsByD1Identifier(String uri, List<String> ids)
            throws IOException, XPathExpressionException, EncoderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<SolrDoc> getDocumentBySolrId(String uri, String id)
            throws IOException, XPathExpressionException, EncoderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<SolrDoc> getDocumentsByResourceMap(String uri,
            String resourceMapId) throws IOException, XPathExpressionException,
            EncoderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<SolrDoc> getDocumentsByField(String uri,
            List<String> fieldValues, String queryField, boolean maxRows)
            throws IOException, XPathExpressionException, EncoderException {
        // TODO Auto-generated method stub
        return new ArrayList<SolrDoc>();
    }

    @Override
    public List<SolrDoc> getDocumentsByResourceMapFieldAndDocumentsField(
            String uri, String resourceMapId, String documentsId)
            throws IOException, XPathExpressionException, EncoderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<SolrDoc> getDocumentsByResourceMapFieldAndIsDocumentedByField(
            String uri, String resourceMapId, String isDocumentedById)
            throws IOException, XPathExpressionException, EncoderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SolrDoc retrieveDocumentFromSolrServer(String id, String solrQueryUri)
            throws XPathExpressionException, IOException, EncoderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setSolrSchemaPath(String path) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setSolrIndexUri(String uri) {
        // TODO Auto-generated method stub

    }

    @Override
    public String getSolrIndexUri() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HttpClient getHttpClient() {
        // TODO Auto-generated method stub
        return null;
    }



}
