package org.dataone.cn.indexer.parser;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;

import java.util.Map;

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
public class ScienceMetadataDocumentSubprocessor extends AbstractDocumentSubprocessor implements IDocumentSubprocessor{



    private IDocumentProvider documentProvider = null;

    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs, Document doc) throws Exception {
        SolrDoc systemMeta = docs.get(identifier);
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
