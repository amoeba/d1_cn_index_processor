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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.resourcemap.ForesiteResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceEntry;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceMapFactory;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementAdd;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dspace.foresite.OREParserException;
import org.springframework.beans.factory.annotation.Autowired;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class IndexTaskDeleteProcessor implements IndexTaskProcessingStrategy {

    private static Logger logger = Logger.getLogger(IndexTaskDeleteProcessor.class.getName());

    @Autowired
    HTTPService httpService;

    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    private HazelcastInstance hzClient;

    private static final String HZ_OBJECT_PATH = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    private IMap<Identifier, String> objectPaths;
    private IMap<Identifier, SystemMetadata> systemMetadata;

    private String solrQueryUri;
    private String solrIndexUri;

    public void process(IndexTask task) throws Exception {
        startHazelClient();
        if (isDataPackage(task)) {
            removeDataPackage(task);
        } else if (isPartOfDataPackage(task)) {
            removeFromDataPackage(task);
        } else {
            removeFromIndex(task);
        }
    }

    private void removeDataPackage(IndexTask task) throws Exception {
        ResourceMap resourceMap = null;
        try {
            resourceMap = ResourceMapFactory.buildResourceMap(task.getObjectPath());
        } catch (OREParserException oreException) {
            logger.warn("Unable to parse ORE document in delete processing for pid: "
                    + task.getPid()
                    + ".  Nothing will be deleted.  Unrecoverable error: task will not be re-tried.");
            return;
        }
        List<String> documentIds = resourceMap.getAllDocumentIDs();
        List<SolrDoc> indexDocuments = httpService.getDocuments(solrQueryUri, documentIds);
        removeFromIndex(task);
        List<SolrDoc> docsToUpdate = new ArrayList<SolrDoc>();
        // for each document in data package:
        for (SolrDoc indexDoc : indexDocuments) {
            if (indexDoc.getIdentifier().equals(task.getPid())) {
                continue; // skipping the resource map, no need update
                          // it.
                          // will
                          // be removed.
            }

            // Remove resourceMap reference
            indexDoc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP,
                    resourceMap.getIdentifier());

            // // Remove documents/documentedby values for this resource
            // map
            for (ResourceEntry entry : resourceMap.getMappedReferences()) {
                if (indexDoc.getIdentifier().equals(entry.getIdentifier())) {
                    for (String documentedBy : entry.getDocumentedBy()) {
                        // Using removeOneFieldWithValue in-case same
                        // documents
                        // are in more than one data package. just
                        // remove
                        // one
                        // instance of data package info.
                        indexDoc.removeOneFieldWithValue(SolrElementField.FIELD_ISDOCUMENTEDBY,
                                documentedBy);
                    }
                    for (String documents : entry.getDocuments()) {
                        indexDoc.removeOneFieldWithValue(SolrElementField.FIELD_DOCUMENTS,
                                documents);
                    }
                    break;
                }
            }
            docsToUpdate.add(indexDoc);
        }
        SolrElementAdd addCommand = new SolrElementAdd(docsToUpdate);
        httpService.sendUpdate(solrIndexUri, addCommand);
    }

    private void removeFromDataPackage(IndexTask task) throws Exception {
        SolrDoc indexedDoc = httpService
                .retrieveDocumentFromSolrServer(task.getPid(), solrQueryUri);

        removeFromIndex(task);
        List<SolrDoc> docsToUpdate = new ArrayList<SolrDoc>();

        List<String> documents = indexedDoc.getAllFieldValues(SolrElementField.FIELD_DOCUMENTS);
        for (String documentsValue : documents) {
            SolrDoc solrDoc = httpService.retrieveDocumentFromSolrServer(documentsValue,
                    solrQueryUri);
            solrDoc.removeFieldsWithValue(SolrElementField.FIELD_ISDOCUMENTEDBY, task.getPid());
            docsToUpdate.add(solrDoc);
        }

        List<String> documentedBy = indexedDoc
                .getAllFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY);
        for (String documentedByValue : documentedBy) {
            SolrDoc solrDoc = httpService.retrieveDocumentFromSolrServer(documentedByValue,
                    solrQueryUri);
            solrDoc.removeFieldsWithValue(SolrElementField.FIELD_DOCUMENTS, task.getPid());
            docsToUpdate.add(solrDoc);
        }

        SolrElementAdd addCommand = new SolrElementAdd(docsToUpdate);
        httpService.sendUpdate(solrIndexUri, addCommand);
    }

    private void removeFromIndex(IndexTask task) {
        httpService.sendSolrDelete(task.getPid());
    }

    private boolean isDataPackage(IndexTask task) {
        return ForesiteResourceMap.representsResourceMap(task.getFormatId());
    }

    private boolean isPartOfDataPackage(IndexTask task) throws XPathExpressionException,
            IOException, EncoderException {
        SolrDoc dataPackageIndexDoc = httpService.retrieveDocumentFromSolrServer(task.getPid(),
                solrQueryUri);
        if (dataPackageIndexDoc != null) {
            String resourceMapId = dataPackageIndexDoc
                    .getFirstFieldValue(SolrElementField.FIELD_RESOURCEMAP);
            return StringUtils.isNotEmpty(resourceMapId);
        } else {
            return false;
        }
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }

    public void setSolrQueryUri(String uri) {
        this.solrQueryUri = uri;
    }

    public void setSolrIndexUri(String uri) {
        this.solrIndexUri = uri;
    }

    private void startHazelClient() {
        if (this.hzClient == null) {
            this.hzClient = HazelcastClientFactory.getStorageClient();
            this.objectPaths = this.hzClient.getMap(HZ_OBJECT_PATH);
            this.systemMetadata = hzClient.getMap(HZ_SYSTEM_METADATA);
        }
    }
}
