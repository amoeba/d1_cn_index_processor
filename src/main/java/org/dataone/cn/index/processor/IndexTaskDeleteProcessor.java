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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.indexer.resourcemap.ForesiteResourceMap;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementAdd;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.springframework.beans.factory.annotation.Autowired;

public class IndexTaskDeleteProcessor implements IndexTaskProcessingStrategy {

    private static Logger logger = Logger.getLogger(IndexTaskDeleteProcessor.class.getName());

    @Autowired
    HTTPService httpService;

    private String solrQueryUri;
    private String solrIndexUri;

    public void process(IndexTask task) throws Exception {
        if (isDataPackage(task)) {
            removeDataPackage(task);
        } else if (isPartOfDataPackage(task)) {
            removeFromDataPackage(task);
        } else {
            removeFromIndex(task);
        }
    }

    private void removeDataPackage(IndexTask task) throws Exception {
        removeFromIndex(task);
        List<SolrDoc> docsToUpdate = getUpdatedSolrDocsByRemovingResourceMap(task.getPid());
        if (docsToUpdate != null && !docsToUpdate.isEmpty()) {
            SolrElementAdd addCommand = new SolrElementAdd(docsToUpdate);
            httpService.sendUpdate(solrIndexUri, addCommand);
        }

    }

    /*
     * Get the list of the solr doc which need to be updated because the removal of the resource map
     */
    private List<SolrDoc> getUpdatedSolrDocsByRemovingResourceMap(String resourceMapId)
            throws XPathExpressionException, IOException, EncoderException {
        List<SolrDoc> updatedSolrDocs = null;
        if (resourceMapId != null && !resourceMapId.trim().equals("")) {
            List<SolrDoc> docsContainResourceMap = httpService.getDocumentsByResourceMap(
                    solrQueryUri, resourceMapId);
            updatedSolrDocs = updateSolrDocsByRemovingResourceMap(docsContainResourceMap,
                    resourceMapId);
        }
        return updatedSolrDocs;
    }

    /*
     * Get the list of the solr doc which need to be updated because the removal of the resource map
     */
    private List<SolrDoc> updateSolrDocsByRemovingResourceMap(List<SolrDoc> docsContainResourceMap,
            String resourceMapId) throws XPathExpressionException, IOException, EncoderException {
        List<SolrDoc> updatedSolrDocs = new ArrayList<SolrDoc>();
        if (docsContainResourceMap != null && docsContainResourceMap.isEmpty()) {
            for (SolrDoc doc : docsContainResourceMap) {
                List<String> resourceMapIdStrs = doc
                        .getAllFieldValues(SolrElementField.FIELD_RESOURCEMAP);
                List<String> dataIdStrs = doc
                        .getAllFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY);
                List<String> metadataIdStrs = doc
                        .getAllFieldValues(SolrElementField.FIELD_DOCUMENTS);
                if ((dataIdStrs == null || dataIdStrs.isEmpty())
                        && (metadataIdStrs == null || metadataIdStrs.isEmpty())) {
                    // only has resourceMap field, doesn't have either documentBy or documents fields.
                    // so we only remove the resource map field.
                    doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP, resourceMapId);
                    updatedSolrDocs.add(doc);
                } else if ((dataIdStrs != null && !dataIdStrs.isEmpty())
                        && (metadataIdStrs == null || metadataIdStrs.isEmpty())) {
                    //data file
                    updatedSolrDocs = handleAggregatedItems(resourceMapId, doc, resourceMapIdStrs,
                            dataIdStrs, SolrElementField.FIELD_ISDOCUMENTEDBY);
                } else if ((dataIdStrs == null || dataIdStrs.isEmpty())
                        && (metadataIdStrs != null && !metadataIdStrs.isEmpty())) {
                    //metadata file
                    updatedSolrDocs = handleAggregatedItems(resourceMapId, doc, resourceMapIdStrs,
                            dataIdStrs, SolrElementField.FIELD_DOCUMENTS);
                }
            }

        }
        return updatedSolrDocs;
    }

    private List<SolrDoc> handleAggregatedItems(String targetResourceMapId, SolrDoc doc,
            List<String> resourceMapIdsInDoc, List<String> aggregatedItemsInDoc, String fieldName) {
        List<SolrDoc> updatedSolrDocs = new ArrayList<SolrDoc>();
        if (doc != null && resourceMapIdsInDoc != null && aggregatedItemsInDoc != null
                && fieldName != null) {
            if (resourceMapIdsInDoc.size() == 1) {
                //only has one resource map. remove the resource map. also remove the documentBy
                doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP, targetResourceMapId);
                doc.removeAllFields(fieldName);
                updatedSolrDocs.add(doc);
            } else if (resourceMapIdsInDoc.size() > 1) {
                if (aggregatedItemsInDoc.size() > 1) {
                    //we have multiple resource maps and multiple documents. We should match them.  					
                    Map<String, String> ids = matchResourceMapsAndItems(doc.getIdentifier(),
                            targetResourceMapId, aggregatedItemsInDoc, fieldName);
                    if (ids != null) {
                        for (String id : ids.keySet()) {
                            doc.removeFieldsWithValue(fieldName, id);
                        }
                    }

                    doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP,
                            targetResourceMapId);
                    updatedSolrDocs.add(doc);

                } else {
                    //multiple resource map aggregate same metadata and data. Just remove the resource map
                    doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP,
                            targetResourceMapId);
                    updatedSolrDocs.add(doc);
                }
            }
        }
        return updatedSolrDocs;
    }

    /*
     * Return a map of mapping aggregation id map the target resourceMapId.
     */
    private Map<String, String> matchResourceMapsAndItems(String targetId,
            String targetResourceMapId, List<String> aggregatedItems, String fieldName) {
        Map<String, String> map = new HashMap<String, String>();
        if (targetId != null && targetResourceMapId != null && aggregatedItems != null
                && fieldName != null) {
            String newFieldName = null;
            if (fieldName.equals(SolrElementField.FIELD_ISDOCUMENTEDBY)) {
                newFieldName = SolrElementField.FIELD_DOCUMENTS;
            } else if (fieldName.equals(SolrElementField.FIELD_DOCUMENTS)) {
                newFieldName = SolrElementField.FIELD_ISDOCUMENTEDBY;
            }
            if (newFieldName != null) {
                for (String item : aggregatedItems) {
                    SolrDoc doc = null;
                    try {
                        doc = getDocumentById(item);
                        List<String> fieldValues = doc.getAllFieldValues(newFieldName);
                        List<String> resourceMapIds = doc
                                .getAllFieldValues(SolrElementField.FIELD_RESOURCEMAP);
                        if ((fieldValues != null && fieldValues.contains(targetId))
                                && (resourceMapIds != null && resourceMapIds
                                        .contains(targetResourceMapId))) {
                            map.put(item, targetResourceMapId);
                        }
                    } catch (Exception e) {
                        logger.warn("IndexTaskDeleteProcessor.matchResourceMapsAndItems - can't get the solrdoc for the id "
                                + item + " since " + e.getMessage());
                    }
                }
            }
        }
        return map;
    }

    private SolrDoc getDocumentById(String id) throws XPathExpressionException, IOException,
            EncoderException {
        SolrDoc doc = null;
        List<SolrDoc> docs = httpService.getDocumentById(solrQueryUri, id);
        if (docs != null && !docs.isEmpty()) {
            doc = docs.get(0);
        }
        return doc;
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

    public void setSolrQueryUri(String uri) {
        this.solrQueryUri = uri;
    }

    public void setSolrIndexUri(String uri) {
        this.solrIndexUri = uri;
    }
}
