package org.dataone.cn.index.processor;

import org.apache.log4j.Logger;
import org.dataone.cn.indexer.resourcemap.IndexVisibilityDelegate;
import org.dataone.service.types.v1.Identifier;

public class IndexVisibilityDelegateHazelcastImpl implements IndexVisibilityDelegate {

    private static Logger logger = Logger.getLogger(IndexVisibilityDelegateHazelcastImpl.class
            .getName());

    public IndexVisibilityDelegateHazelcastImpl() {
    }

    public boolean isDocumentVisible(Identifier pid) {
        //        boolean visible = false;
        //        try {
        //            
        //            SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(pid);
        //            if (SolrDoc.visibleInIndex(systemMetadata)) {
        //                visible = true;
        //            }
        //        } catch (NullPointerException npe) {
        //            logger.warn("Could not get visible value for pid: " + pid.getValue());
        //        }
        //        return visible;
        return true;
    }

    public boolean documentExists(Identifier pid) {
        //        boolean exists = false;
        //        try {
        //            SystemMetadata systemMetadata = HazelcastClientFactory.getSystemMetadataMap().get(pid);
        //            if (systemMetadata != null) {
        //                exists = true;
        //            }
        //        } catch (NullPointerException npe) {
        //            logger.warn("Could not get visible value for pid: " + pid.getValue());
        //        }
        //        return exists;
        return true;
    }
}
