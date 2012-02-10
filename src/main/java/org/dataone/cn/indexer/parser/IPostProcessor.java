package org.dataone.cn.indexer.parser;

import org.dataone.cn.indexer.solrhttp.SolrDoc;

import java.util.Map;

/**
 * User: porter
 * Date: 10/26/11
 */

public interface IPostProcessor {
    public Map<String, SolrDoc> process(String identifier, Map<String, SolrDoc> docMap) ;

}
