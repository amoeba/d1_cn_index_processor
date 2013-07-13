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

import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

public class ForesiteResourceEntry implements ResourceEntry {

    /* Instance variables */
    private String identifier = null;
    private Set<String> resourceMaps = null;
    private ForesiteResourceMap parentMap = null;
    private Set<String> documents = null;
    private Set<String> documentedBy = null;

    /**
     * Public constructor
     */
    public ForesiteResourceEntry(String identifier, ForesiteResourceMap parentMap) {
        this.setIdentifier(identifier);
        this.setResourceMaps(new HashSet<String>());
        this.setParentMap(parentMap);
        this.setDocuments(new HashSet<String>());
        this.setDocumentedBy(new HashSet<String>());
    }

    @Override
    public Set<String> getResourceMaps() {
        return this.resourceMaps;
    }

    @Override
    public void setResourceMaps(Set<String> resourceMaps) {
        this.resourceMaps = resourceMaps;
    }

    @Override
    public String getIdentifier() {
        return this.identifier;
    }

    @Override
    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public Set<String> getDocuments() {
        return this.documents;
    }

    void setDocuments(Set<String> documents) {
        this.documents = documents;
    }

    public void addDocuments(String documents) {
        this.documents.add(documents);
    }

    @Override
    public Set<String> getDocumentedBy() {
        return this.documentedBy;
    }

    void setDocumentedBy(Set<String> documentedBy) {
        this.documentedBy = documentedBy;
    }

    public void addDocumentedBy(String documentedBy) {
        this.documentedBy.add(documentedBy);
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();

        sw.write("\tRESOURCE MAP ENTITY: ");
        sw.append(this.identifier);
        sw.write("\n");

        sw.write("\t\tDocuments: \n");
        for (String documentString : this.documents) {
            sw.write("\t\t\t");
            sw.append(documentString);
            sw.write("\n");
        }

        sw.write("\t\tisDocumentedByString: \n");
        for (String isDocumentedByString : this.documentedBy) {
            sw.write("\t\t\t");
            sw.append(isDocumentedByString);
            sw.write("\n");
        }

        sw.write("\t\tResource Maps: \n");
        for (String resourceMap : resourceMaps) {
            sw.write("\t\t\t");
            sw.append(resourceMap);
            sw.write("\n");
        }

        return sw.toString();
    }

    @Override
    public ResourceMap getParentMap() {
        return this.parentMap;
    }

    void setParentMap(ResourceMap parentMap) {
        this.parentMap = (ForesiteResourceMap) parentMap;
    }
}
