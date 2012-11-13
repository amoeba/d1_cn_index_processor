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

import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 8/29/11
 * Time: 10:15 AM
 */

/**
 * ResourceEntry is an document entry in the Resource Map. Used for parsing
 * information about entry and defining bidirectional references to other
 * documents in the index.
 * 
 */
public class ResourceEntry {
    private Element entry = null;

    private String identifier = null;

    private Set<String> resourceMaps = new HashSet<String>();
    private Set<String> documents = new HashSet<String>();
    private Set<String> isDocumentedBy = new HashSet<String>();

    private String about = null;
    private ResourceMap parentMap = null;

    private HazelcastInstance hzClient;
    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");
    private IMap<Identifier, SystemMetadata> systemMetadata;

    public ResourceEntry(Element entry, ResourceMap parentMap) {
        getResourceMaps().add(parentMap.getIdentifier());
        this.setParentMap(parentMap);
        this.entry = entry;
        initEntry();
    }

    private void initEntry() {
        startHazelClient();
        setAbout(entry.getAttributeNS(ResourceMap.NS_RDF, ResourceMap.ATTRIBUTE_ABOUT));
        setIdentifier(entry
                .getElementsByTagNameNS(ResourceMap.NS_DCTERMS, ResourceMap.TAG_IDENTIFIER).item(0)
                .getTextContent());

        NodeList nlDocuments = entry.getElementsByTagNameNS(ResourceMap.NS_CITO,
                ResourceMap.TAG_DOCUMENTS);
        documents = parseDocuments(nlDocuments);
        NodeList nlIsDocumentedBy = entry.getElementsByTagNameNS(ResourceMap.NS_CITO,
                ResourceMap.TAG_IS_DOCUMENTED_BY);
        isDocumentedBy = parseIsDocumentedBy(nlIsDocumentedBy);

    }

    private Set<String> parseIsDocumentedBy(NodeList nlIsDocumentedBy) {
        Set<String> isDocumentedByStrings = new HashSet<String>();
        for (int i = 0; i < nlIsDocumentedBy.getLength(); i++) {
            Element isDocumentedByElement = (Element) nlIsDocumentedBy.item(i);
            String isDocumentedByString = isDocumentedByElement.getAttributeNS(ResourceMap.NS_RDF,
                    ResourceMap.ATTRIBUTE_RESOURCE);
            String id = parentMap.getIdentifierFromResource(isDocumentedByString);
            Identifier pid = new Identifier();
            pid.setValue(id);
            SystemMetadata smd = systemMetadata.get(pid);
            if (SolrDoc.visibleInIndex(smd)) {
                isDocumentedByStrings.add(id);
            }
        }

        return isDocumentedByStrings;
    }

    private Set<String> parseDocuments(NodeList nlDocuments) {
        Set<String> documetsStrings = new HashSet<String>();
        for (int i = 0; i < nlDocuments.getLength(); i++) {
            Element documentsElement = (Element) nlDocuments.item(i);
            String resource = documentsElement.getAttributeNS(ResourceMap.NS_RDF,
                    ResourceMap.ATTRIBUTE_RESOURCE);
            String id = parentMap.getIdentifierFromResource(resource);
            Identifier pid = new Identifier();
            pid.setValue(id);
            SystemMetadata smd = systemMetadata.get(pid);
            if (SolrDoc.visibleInIndex(smd)) {
                documetsStrings.add(id);
            }

        }
        return documetsStrings;
    }

    public Element getEntry() {
        return entry;
    }

    public void setEntry(Element entry) {
        this.entry = entry;
    }

    public Set<String> getResourceMaps() {
        if (resourceMaps == null) {
            resourceMaps = new HashSet<String>();
        }
        return resourceMaps;
    }

    public void setResourceMaps(Set<String> resourceMaps) {
        this.resourceMaps = resourceMaps;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Set<String> getDocuments() {
        return documents;
    }

    public void setDocuments(Set<String> documents) {
        this.documents = documents;
    }

    public Set<String> getDocumentedBy() {
        return isDocumentedBy;
    }

    public void setDocumentedBy(Set<String> documentedBy) {
        isDocumentedBy = documentedBy;
    }

    public String getAbout() {
        return about;
    }

    public void setAbout(String about) {
        this.about = about;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();

        sw.write("\tRESOURCE MAP ENTITY: ");
        sw.append(identifier);
        sw.write("\n");

        sw.write("\t\tDocuments: ");
        sw.write("\n");
        for (String documentString : documents) {
            sw.write("\t\t\t");
            sw.append(documentString);
            sw.write("\n");
        }
        sw.write("\t\tisDocumentedByString: ");
        sw.write("\n");
        for (String isDocumentedByString : isDocumentedBy) {
            sw.write("\t\t\t");
            sw.append(isDocumentedByString);
            sw.write("\n");
        }

        sw.write("\t\tResource Maps: ");
        ;
        sw.write("\n");
        for (String resourceMap : resourceMaps) {
            sw.write("\t\t\t");
            sw.append(resourceMap);
            sw.write("\n");
        }

        return sw.toString();

    }

    public ResourceMap getParentMap() {
        return parentMap;
    }

    public void setParentMap(ResourceMap parentMap) {
        this.parentMap = parentMap;
    }

    private void startHazelClient() {
        if (this.hzClient == null) {
            this.hzClient = HazelcastClientFactory.getStorageClient();
            this.systemMetadata = this.hzClient.getMap(HZ_SYSTEM_METADATA);
        }
    }
}
