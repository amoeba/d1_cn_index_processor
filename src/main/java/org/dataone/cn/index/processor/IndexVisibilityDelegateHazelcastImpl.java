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
     * Checks the archived attribute of the system metadata
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
