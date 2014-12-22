package org.dataone.cn.indexer.parser;

import java.util.Map;

import org.dataone.cn.indexer.solrhttp.SolrDoc;

public interface IDocumentDeleteSubprocessor {

    public Map<String, SolrDoc> processDocForDelete(String identifier, Map<String, SolrDoc> docs)
            throws Exception;
}
