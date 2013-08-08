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

package org.dataone.cn.indexer.resourcemap;

import org.dataone.service.types.v1.Identifier;

/**
 * Interface definition regarding what documents are placed in the index (visible).
 * Also externalizes hazelcast dependency in resource map processing through the 
 * documentExists method - which in the hazelcast implementation ensures the pid
 * is present in the system metadata data structure.
 * 
 * @author sroseboo
 *
 */
public interface IndexVisibilityDelegate {

    public boolean isDocumentVisible(Identifier pid);

    public boolean documentExists(Identifier pid);

}
