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


import javax.xml.namespace.NamespaceContext;
import java.util.*;

/**
  * User: Porter
 * Date: 7/25/11
 * Time: 4:32 PM
 */
public class XMLNamespaceConfig implements NamespaceContext {

    /** Used in XPathDocumentParser holds a list of prefixes and namespaces for use in XPath Queries
     *
     * @see SolrIndexService
     */

    private List<XMLNamespace> namespaceList = null;

    /**
     * @param namespaceList
     */
    public XMLNamespaceConfig(List<XMLNamespace> namespaceList){
        this.namespaceList = namespaceList;
    }

    public String getNamespaceURI(String prefix) {
        for (XMLNamespace xmlNamespace : namespaceList) {
            if(xmlNamespace.getPrefix().equals(prefix)){
                return xmlNamespace.getNamespace();
            }
        }
        return null;
    }

    public String getPrefix(String namespaceURI) {
        for (XMLNamespace xmlNamespace : namespaceList) {
            if(xmlNamespace.getNamespace().equals(namespaceURI)){
                return xmlNamespace.getPrefix();
            }
        }
        return null;
    }

    private Set prefixes = null;
    public Iterator getPrefixes(String namespaceURI) {

        if(prefixes == null){
            prefixes = new HashSet();
            for (XMLNamespace xmlNamespace : namespaceList) {
                prefixes.add(xmlNamespace.getPrefix());
            }
        }
        return prefixes.iterator();
    }


}
