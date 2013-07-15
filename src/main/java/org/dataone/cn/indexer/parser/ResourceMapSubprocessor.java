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

package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceMapFactory;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dspace.foresite.OREParserException;
import org.w3c.dom.Document;

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
            Document doc) throws XPathExpressionException, OREParserException, IOException,
            EncoderException {
        SolrDoc resourceMapDoc = docs.get(identifier);
        List<SolrDoc> processedDocs = processResourceMap(resourceMapDoc, doc);
        Map<String, SolrDoc> processedDocsMap = new HashMap<String, SolrDoc>();
        for (SolrDoc processedDoc : processedDocs) {
            processedDocsMap.put(processedDoc.getIdentifier(), processedDoc);
        }
        return processedDocsMap;
    }

    private List<SolrDoc> processResourceMap(SolrDoc indexDocument, Document resourceMapDocument)
            throws OREParserException, XPathExpressionException, IOException, EncoderException {

        ResourceMap resourceMap = ResourceMapFactory.buildResourceMap(resourceMapDocument);
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
