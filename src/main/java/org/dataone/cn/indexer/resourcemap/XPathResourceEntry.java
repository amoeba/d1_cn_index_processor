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
public class XPathResourceEntry implements ResourceEntry {
    private Element entry = null;

    private String identifier = null;

    private Set<String> resourceMaps = new HashSet<String>();
    private Set<String> documents = new HashSet<String>();
    private Set<String> isDocumentedBy = new HashSet<String>();

    private String about = null;
    private XPathResourceMap parentMap = null;

    private HazelcastInstance hzClient;
    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");
    private IMap<Identifier, SystemMetadata> systemMetadata;

    public XPathResourceEntry(Element entry, ResourceMap parentMap) {
        getResourceMaps().add(parentMap.getIdentifier());
        this.setParentMap(parentMap);
        this.entry = entry;
        initEntry();
    }

    private void initEntry() {
        startHazelClient();
        setAbout(entry.getAttributeNS(XPathResourceMap.NS_RDF, XPathResourceMap.ATTRIBUTE_ABOUT));
        setIdentifier(entry
                .getElementsByTagNameNS(XPathResourceMap.NS_DCTERMS, XPathResourceMap.TAG_IDENTIFIER).item(0)
                .getTextContent());

        NodeList nlDocuments = entry.getElementsByTagNameNS(XPathResourceMap.NS_CITO,
                XPathResourceMap.TAG_DOCUMENTS);
        documents = parseDocuments(nlDocuments);
        NodeList nlIsDocumentedBy = entry.getElementsByTagNameNS(XPathResourceMap.NS_CITO,
                XPathResourceMap.TAG_IS_DOCUMENTED_BY);
        isDocumentedBy = parseIsDocumentedBy(nlIsDocumentedBy);

    }

    private Set<String> parseIsDocumentedBy(NodeList nlIsDocumentedBy) {
        Set<String> isDocumentedByStrings = new HashSet<String>();
        for (int i = 0; i < nlIsDocumentedBy.getLength(); i++) {
            Element isDocumentedByElement = (Element) nlIsDocumentedBy.item(i);
            String isDocumentedByString = isDocumentedByElement.getAttributeNS(XPathResourceMap.NS_RDF,
                    XPathResourceMap.ATTRIBUTE_RESOURCE);
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
            String resource = documentsElement.getAttributeNS(XPathResourceMap.NS_RDF,
                    XPathResourceMap.ATTRIBUTE_RESOURCE);
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

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getEntry()
	 */
	Element getEntry() 
	{
        return entry;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setEntry(org.w3c.dom.Element)
	 */
	void setEntry(Element entry) 
    {
        this.entry = entry;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getResourceMaps()
	 */
    @Override
	public Set<String> getResourceMaps() {
        if (resourceMaps == null) {
            resourceMaps = new HashSet<String>();
        }
        return resourceMaps;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setResourceMaps(java.util.Set)
	 */
    @Override
	public void setResourceMaps(Set<String> resourceMaps) {
        this.resourceMaps = resourceMaps;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getIdentifier()
	 */
    @Override
	public String getIdentifier() {
        return identifier;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setIdentifier(java.lang.String)
	 */
    @Override
	public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getDocuments()
	 */
    @Override
	public Set<String> getDocuments() {
        return documents;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setDocuments(java.util.Set)
	 */
    void setDocuments(Set<String> documents) {
        this.documents = documents;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getDocumentedBy()
	 */
    @Override
	public Set<String> getDocumentedBy() {
        return isDocumentedBy;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setDocumentedBy(java.util.Set)
	 */
	void setDocumentedBy(Set<String> documentedBy) {
        isDocumentedBy = documentedBy;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getAbout()
	 */
	public String getAbout() {
        return about;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setAbout(java.lang.String)
	 */
    void setAbout(String about) {
        this.about = about;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#toString()
	 */
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

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#getParentMap()
	 */
    @Override
	public ResourceMap getParentMap() {
        return parentMap;
    }

    /* (non-Javadoc)
	 * @see org.dataone.cn.indexer.resourcemap.ResourceEntry#setParentMap(org.dataone.cn.indexer.resourcemap.ResourceMap)
	 */
	void setParentMap(ResourceMap parentMap) {
        this.parentMap = (XPathResourceMap)parentMap;
    }

    private void startHazelClient() {
        if (this.hzClient == null) {
            this.hzClient = HazelcastClientFactory.getStorageClient();
            this.systemMetadata = this.hzClient.getMap(HZ_SYSTEM_METADATA);
        }
    }
}
