package org.dataone.cn.index.processor;

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.indexer.resourcemap.IndexVisibilityDelegate;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;

import com.hazelcast.core.IMap;

public class IndexVisibilityDelegateHazelcastImpl implements IndexVisibilityDelegate {

    private static Logger logger = Logger.getLogger(IndexVisibilityDelegateHazelcastImpl.class
            .getName());

    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    public IndexVisibilityDelegateHazelcastImpl() {
    }

    public boolean isDocumentVisible(Identifier pid) {
        boolean visible = false;
        try {
            IMap<Identifier, SystemMetadata> systemMetadataMap = HazelcastClientFactory
                    .getStorageClient().getMap(HZ_SYSTEM_METADATA);
            SystemMetadata systemMetadata = systemMetadataMap.get(pid);
            if (SolrDoc.visibleInIndex(systemMetadata)) {
                visible = true;
            }
        } catch (NullPointerException npe) {
            logger.warn("Could not get visible value for pid: " + pid.getValue());
        }
        return visible;
    }
}
