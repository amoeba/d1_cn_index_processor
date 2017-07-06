package org.dataone.cn.index.processor;

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.indexer.resourcemap.IndexVisibilityDelegate;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;

public class IndexVisibilityDelegateHazelcastImpl implements IndexVisibilityDelegate {

    private static Logger logger = Logger.getLogger(IndexVisibilityDelegateHazelcastImpl.class
            .getName());

    public IndexVisibilityDelegateHazelcastImpl() {
    }
    
    /**
     * @param id - if sid, it will always return true
     */
    public boolean isDocumentVisible(Identifier id) {
        boolean visible = false;
        try {

            SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(id);
            // TODO: Is pid Identifier a SID?
            if (systemMetadata == null) {
                return true;
            }
            if (SolrDoc.visibleInIndex(systemMetadata)) {
                visible = true;
            }
        } catch (NullPointerException npe) {
            logger.warn("Could not determine isDocumentVisible for id: " + id.getValue());
        }
        return visible;
    }

    // TODO:  this routine does nothing!  always returns true unless there's a null pointer exception
    /**
     * Returns true if there is systemMetadata for the identifier
     */
    public boolean documentExists(Identifier id) {
        boolean exists = false;
        try {
            SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(id);
            if (systemMetadata != null) {
                exists = true;
            } else {
                // TODO: if id is a sid, we could get here
                // what are the semantics of sid visibility?
                return true;
            }
        } catch (NullPointerException npe) {
            logger.warn("Could not determine if documentExists for id: " + id.getValue());
        }
        return exists;
    }
}
