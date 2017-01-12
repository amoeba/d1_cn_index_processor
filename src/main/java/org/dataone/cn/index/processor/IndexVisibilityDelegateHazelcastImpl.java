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
     * @param pid - will not work if the identifier is a SID  (will always return true)
     */
    public boolean isDocumentVisible(Identifier pid) {
        boolean visible = false;
        try {

            SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(pid);
            // TODO: Is pid Identifier a SID?
            if (systemMetadata == null) {
                return true;
            }
            if (SolrDoc.visibleInIndex(systemMetadata)) {
                visible = true;
            }
        } catch (NullPointerException npe) {
            logger.warn("Could not determine isDocumentVisible for pid: " + pid.getValue());
        }
        return visible;
    }

    // TODO:  this routine does nothing!  always returns true unless there's a null pointer exception
    public boolean documentExists(Identifier pid) {
        boolean exists = false;
        try {
            SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(pid);
            if (systemMetadata != null) {
                exists = true;
            } else {
                // TODO: Is pid Identifier a SID?
                return true;
            }
        } catch (NullPointerException npe) {
            logger.warn("Could not determine if documentExists for pid: " + pid.getValue());
        }
        return exists;
    }
}
