/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

package org.dataone.cn.indexer;

import java.io.IOException;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.http.client.HttpClient;
import org.dataone.cn.indexer.solrhttp.SolrDoc;

public interface D1IndexerSolrClient {



    public void sendUpdate(String uri, List<SolrDoc> data) 
            throws IOException;
    
    
    public void sendUpdate(String uri, List<SolrDoc> data, String encoding)
            throws IOException;

   
    /**
     * Posts document data to Solr indexer.
     * 
     * @param uri
     *            Solr index url example:
     *            http://localhost:8983/solr/update?commit=true
     * @param data
     *            documents to index
     * @param encoding
     *            use "UTF-8"
     * @param isPartialUpdate
     *            use atomic/partial update semantics  (field modifiers)
     *            
     * @throws IOException
     */
    public void sendUpdate(String uri, List<SolrDoc> data, String encoding, boolean isPartialUpdate) 
            throws IOException;

    
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

    /**
     * sets the path to the solr schema.  Implementations may act on the path by 
     * loading the schema for use, so opportunity for Exceptions being thrown in allowed.
     * @param path
     * @throws Exception - generally of the variety of FileNotFound or some sort of parsing error
     */
    public void setSolrSchemaPath(String path) throws Exception;

    public void setSolrIndexUri(String uri);

    public String getSolrIndexUri();

    public HttpClient getHttpClient();

}