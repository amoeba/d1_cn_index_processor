package org.dataone.cn.indexer.resourcemap;

import java.util.List;
import java.util.Set;

import org.dataone.cn.indexer.solrhttp.SolrDoc;

public interface ResourceMap {
    public List<String> getAllDocumentIDs();

    public Set<String> getContains();

    public String getIdentifier();

    public Set<ResourceEntry> getMappedReferences();

    public List<SolrDoc> mergeIndexedDocuments(List<SolrDoc> docs);

    public void setIndexVisibilityDeledate(IndexVisibilityDelegate ivd);
}