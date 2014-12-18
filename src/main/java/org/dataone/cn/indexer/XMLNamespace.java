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

package org.dataone.cn.indexer;

/**
  * User: Porter
 * Date: 7/25/11
 * Time: 1:28 PM
 */

/**Helper class for prefix to namespace mappings used in xpath
 *
 */
public class XMLNamespace {

    /**Used to define prefix and namespace for XMLNamespace for xpath queries in XPathDocumentParser
     *
     * @see XMLNamespace
     * @see SolrIndexService
     */

    private String prefix = null;
    private String namespace = null;

    public XMLNamespace() {
    }

    public XMLNamespace(String prefix, String namespace) {
        this.prefix = prefix;
        this.namespace = namespace;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
