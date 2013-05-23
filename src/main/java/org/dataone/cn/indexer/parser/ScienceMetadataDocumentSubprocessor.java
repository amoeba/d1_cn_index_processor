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

import java.util.Map;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/22/11
 * Time: 1:41 PM
 */

/**Retrieves science metadata document from ID using {@link IDocumentProvider}.  After retrievval document is processed
 * and fields added to {@link SolrDoc} to be indexed.
 *
 */
public class ScienceMetadataDocumentSubprocessor extends AbstractDocumentSubprocessor implements
        IDocumentSubprocessor {

    private IDocumentProvider documentProvider = null;

    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            Document doc) throws Exception {
        Document sciMetaDoc = null;
        if (documentProvider == null) {
            sciMetaDoc = doc;
        } else {
            sciMetaDoc = documentProvider.GetDocument(identifier);
        }
        return super.processDocument(identifier, docs, sciMetaDoc);
    }

    public IDocumentProvider getDocumentProvider() {
        return documentProvider;
    }

    public void setDocumentProvider(IDocumentProvider documentProvider) {
        this.documentProvider = documentProvider;
    }
}
