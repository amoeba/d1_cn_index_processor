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

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.log4j.Logger;
import org.dataone.cn.index.processor.IndexVisibilityDelegateHazelcastImpl;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.ore.ResourceMapFactory;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREException;
import org.dspace.foresite.OREParserException;
import org.w3c.dom.Document;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;

public class ForesiteResourceMap implements ResourceMap {
    /* Class contants */
    private static final String RESOURCE_MAP_FORMAT = "http://www.openarchives.org/ore/terms";
    private static Logger logger = Logger.getLogger(ForesiteResourceMap.class.getName());

    /* Instance variables */
    private String identifier = null;
    private HashMap<String, ForesiteResourceEntry> resourceMap = null;

    private IndexVisibilityDelegate indexVisibilityDelegate = new IndexVisibilityDelegateHazelcastImpl();

    public ForesiteResourceMap(String fileObjectPath) throws OREParserException {
        FileInputStream fileInputStream = null;
        try {
            fileInputStream = new FileInputStream(fileObjectPath);
            _init(fileInputStream);
        } catch (Exception e) {
            throw new OREParserException(e);
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            } catch (IOException e) {
                logger.error("error closing file input stream", e);
            }
        }
    }

    public ForesiteResourceMap(Document doc) throws OREParserException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InputStream is = null;
        try {
            DOMImplementationLS domImpl = null;
            try {
                domImpl = (DOMImplementationLS) (DOMImplementationRegistry.newInstance()
                        .getDOMImplementation("LS"));
                LSOutput lsOutput = domImpl.createLSOutput();
                lsOutput.setEncoding("UTF-8");
                lsOutput.setByteStream(bos);
                LSSerializer lsSerializer = domImpl.createLSSerializer();
                lsSerializer.write(doc, lsOutput);
                is = new ReaderInputStream(new StringReader(bos.toString()));

                this._init(is);
            } catch (Exception e) {
                throw new OREParserException(e);
            }
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                throw new OREParserException(e);
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    throw new OREParserException(e);
                }
            }
        }
    }

    /**
     * Constructor for testing, allows test class to override the index visibility delegate object.  
     * To avoid need for hazelcast during testing.
     * @param doc
     * @param ivd
     * @throws OREException
     * @throws URISyntaxException
     * @throws OREParserException
     * @throws IOException
     */
    public ForesiteResourceMap(String doc, IndexVisibilityDelegate ivd) throws OREException,
            URISyntaxException, OREParserException, IOException {
        InputStream is = new ReaderInputStream(new StringReader(doc));
        if (ivd != null) {
            this.indexVisibilityDelegate = ivd;
        }
        try {
            _init(is);
        } finally {
            is.close();
        }
    }

    private void _init(InputStream is) throws OREException, URISyntaxException,
            UnsupportedEncodingException, OREParserException {
        /* Creates the identifier map from the doc */
        Map<Identifier, Map<Identifier, List<Identifier>>> tmpResourceMap = null;

        try {
            tmpResourceMap = ResourceMapFactory.getInstance().parseResourceMap(is);

        } catch (Throwable e) {
            logger.error("Unable to parse ORE document:", e);
            throw new OREParserException(e);
        }

        /* Gets the top level identifier */
        Identifier identifier = tmpResourceMap.keySet().iterator().next();
        this.setIdentifier(identifier.getValue());

       
        /* Gets the to identifier map */
        Map<Identifier, List<Identifier>> identiferMap = tmpResourceMap.get(identifier);

        this.resourceMap = new HashMap<String, ForesiteResourceEntry>();

        for(Map.Entry<Identifier, List<Identifier>> entry : identiferMap.entrySet()) 
        {
            ForesiteResourceEntry documentsResourceEntry = resourceMap.get(entry.getKey()
                    .getValue());

            if (documentsResourceEntry == null) {
                documentsResourceEntry = new ForesiteResourceEntry(entry.getKey().getValue(), this);
                resourceMap.put(entry.getKey().getValue(), documentsResourceEntry);
            }

            for (Identifier documentedByIdentifier : entry.getValue()) {

                Identifier pid = new Identifier();
                pid.setValue(documentedByIdentifier.getValue());
                if (indexVisibilityDelegate.isDocumentVisible(pid)) {
                    documentsResourceEntry.addDocuments(documentedByIdentifier.getValue());
                }

                ForesiteResourceEntry documentedByResourceEntry = resourceMap
                        .get(documentedByIdentifier.getValue());

                if (documentedByResourceEntry == null) {
                    documentedByResourceEntry = new ForesiteResourceEntry(
                            documentedByIdentifier.getValue(), this);

                    resourceMap.put(documentedByIdentifier.getValue(), documentedByResourceEntry);
                }

                pid = new Identifier();
                pid.setValue(entry.getKey().getValue());

                if (indexVisibilityDelegate.isDocumentVisible(pid)) {
                    documentedByResourceEntry.addDocumentedBy(entry.getKey().getValue());
                }
            }
        }
    }

    public static boolean representsResourceMap(String formatId) {
        return RESOURCE_MAP_FORMAT.equals(formatId);
    }

    private SolrDoc _mergeMappedReference(ResourceEntry resourceEntry, SolrDoc mergeDocument) {

        if (mergeDocument.hasField(SolrElementField.FIELD_ID) == false) {
            mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_ID, resourceEntry
                    .getIdentifier()));
        }

        for (String documentedBy : resourceEntry.getDocumentedBy()) {
            if (mergeDocument
                    .hasFieldWithValue(SolrElementField.FIELD_ISDOCUMENTEDBY, documentedBy) == false) {
                mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_ISDOCUMENTEDBY,
                        documentedBy));
            }
        }

        for (String documents : resourceEntry.getDocuments()) {
            if (mergeDocument.hasFieldWithValue(SolrElementField.FIELD_DOCUMENTS, documents) == false) {
                mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_DOCUMENTS,
                        documents));
            }
        }

        for (String resourceMap : resourceEntry.getResourceMaps()) {
            if (mergeDocument.hasFieldWithValue(SolrElementField.FIELD_RESOURCEMAP, resourceMap) == false) {
                mergeDocument.addField(new SolrElementField(SolrElementField.FIELD_RESOURCEMAP,
                        resourceMap));
            }
        }

        mergeDocument.setMerged(true);

        return mergeDocument;
    }

    public Set<ResourceEntry> getMappedReferences() {
        /* Builds a set for references that are visible in solr doc index and
         * are not the resource map id */
        HashSet<ResourceEntry> resourceEntries = new HashSet<ResourceEntry>();

        for (ResourceEntry resourceEntry : this.resourceMap.values()) {
            Identifier pid = new Identifier();
            pid.setValue(resourceEntry.getIdentifier());
            // if the document does not have system metadata yet, cannot check visibility.  include in list of ids.
            // if document does exist, it must be visible in the index to be included.
            if (!indexVisibilityDelegate.documentExists(pid)
                    || indexVisibilityDelegate.isDocumentVisible(pid)) {
                if (resourceEntry.getIdentifier().equals(this.getIdentifier()) == false) {
                    resourceEntries.add(resourceEntry);
                }
            }
        }

        /* Return the set of resource entries */
        return resourceEntries;
    }

    @Override
    public Set<String> getContains() {
        Set<String> contains = new HashSet<String>();

        for (String id : this.resourceMap.keySet()) {
            contains.add(id);
        }

        for (ResourceEntry resourceEntry : this.resourceMap.values()) {
            contains.add(resourceEntry.getIdentifier());
        }

        return contains;
    }

    @Override
    public List<String> getAllDocumentIDs() {
        List<String> docIds = new LinkedList<String>();

        /* Adds the map identifier */
        docIds.add(this.getIdentifier());

        /* Adds the mapped references */
        for (ResourceEntry resourceEntry : getMappedReferences()) {
            docIds.add(resourceEntry.getIdentifier());
        }

        /* Return the document IDs */
        return docIds;
    }

    @Override
    public List<SolrDoc> mergeIndexedDocuments(List<SolrDoc> docs) {

        List<SolrDoc> mergedDocuments = new ArrayList<SolrDoc>();

        for (ResourceEntry resourceEntry : this.resourceMap.values()) {
            for (SolrDoc doc : docs) {
                if (doc.getIdentifier().equals(resourceEntry.getIdentifier())) {
                    mergedDocuments.add(_mergeMappedReference(resourceEntry, doc));
                    break;
                }
            }
        }

        return mergedDocuments;
    }

    @Override
    public String getIdentifier() {
        return this.identifier;
    }

    void setIdentifier(String identifier) {
        this.identifier = identifier;
    }
}