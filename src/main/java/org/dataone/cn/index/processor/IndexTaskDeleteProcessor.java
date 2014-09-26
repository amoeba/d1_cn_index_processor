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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.collections.CollectionUtils;
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
            updatedSolrDocs = removeResourceMapRelationship(docsContainResourceMap,
                    resourceMapId);
        }
        return updatedSolrDocs;
    }

    /*
     * Get the list of the solr doc which need to be updated because the removal of the resource map
     */
    private List<SolrDoc> removeResourceMapRelationship(List<SolrDoc> docsContainResourceMap,
            String resourceMapId) throws XPathExpressionException, IOException, EncoderException {
        List<SolrDoc> totalUpdatedSolrDocs = new ArrayList<SolrDoc>();
        if (docsContainResourceMap != null && !docsContainResourceMap.isEmpty()) {
            for (SolrDoc doc : docsContainResourceMap) {
                List<SolrDoc> updatedSolrDocs = new ArrayList<SolrDoc>();
                List<String> resourceMapIdStrs = doc
                        .getAllFieldValues(SolrElementField.FIELD_RESOURCEMAP);
                List<String> dataIdStrs = doc
                        .getAllFieldValues(SolrElementField.FIELD_DOCUMENTS);
                List<String> metadataIdStrs = doc
                        .getAllFieldValues(SolrElementField.FIELD_ISDOCUMENTEDBY);
                if ((dataIdStrs == null || dataIdStrs.isEmpty())
                        && (metadataIdStrs == null || metadataIdStrs.isEmpty())) {
                    // only has resourceMap field, doesn't have either documentBy or documents fields.
                    // so we only remove the resource map field.
                    doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP, resourceMapId);
                    updatedSolrDocs.add(doc);
                } else if ((dataIdStrs != null && !dataIdStrs.isEmpty())
                        && (metadataIdStrs == null || metadataIdStrs.isEmpty())) {
                    //The solr doc is for a metadata object since the solr doc documents data files
                    updatedSolrDocs = removeAggregatedItems(resourceMapId, doc, resourceMapIdStrs,
                            dataIdStrs, SolrElementField.FIELD_DOCUMENTS);
                } else if ((dataIdStrs == null || dataIdStrs.isEmpty())
                        && (metadataIdStrs != null && !metadataIdStrs.isEmpty())) {
                    //The solr doc is for a data object since it documentedBy elements.
                    updatedSolrDocs = removeAggregatedItems(resourceMapId, doc, resourceMapIdStrs,
                            metadataIdStrs, SolrElementField.FIELD_ISDOCUMENTEDBY);
                } else if ((dataIdStrs != null && !dataIdStrs.isEmpty())
                        && (metadataIdStrs != null && !metadataIdStrs.isEmpty())){
                    // both metadata and data for one object
                    List<SolrDoc> solrDocsRemovedDocuments = removeAggregatedItems(resourceMapId, doc, resourceMapIdStrs,
                            dataIdStrs, SolrElementField.FIELD_DOCUMENTS);
                    List<SolrDoc> solrDocsRemovedDocumentBy = removeAggregatedItems(resourceMapId, doc, resourceMapIdStrs,
                            metadataIdStrs, SolrElementField.FIELD_ISDOCUMENTEDBY);
                    updatedSolrDocs = mergeUpdatedSolrDocs(solrDocsRemovedDocumentBy, solrDocsRemovedDocuments);
                }
                //move them to the final result
                if(updatedSolrDocs != null) {
                    for(SolrDoc updatedDoc: updatedSolrDocs) {
                        totalUpdatedSolrDocs.add(updatedDoc);
                    }
                }
                
            }

        }
        return totalUpdatedSolrDocs;
    }
    
    /*
     * Process the list of ids of the documentBy/documents in a slor doc.
     */
    private List<SolrDoc> removeAggregatedItems(String targetResourceMapId, SolrDoc doc,
            List<String> resourceMapIdsInDoc, List<String> aggregatedItemsInDoc, String fieldNameRemoved) {
        List<SolrDoc> updatedSolrDocs = new ArrayList<SolrDoc>();
        if (doc != null && resourceMapIdsInDoc != null && aggregatedItemsInDoc != null
                && fieldNameRemoved != null) {
            if (resourceMapIdsInDoc.size() == 1) {
                //only has one resource map. remove the resource map. also remove the documentBy
                doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP, targetResourceMapId);
                doc.removeAllFields(fieldNameRemoved);
                updatedSolrDocs.add(doc);
            } else if (resourceMapIdsInDoc.size() > 1) {
            	//we have multiple resource maps. We should match them.  					
                Map<String, String> ids = matchResourceMapsAndItems(doc.getIdentifier(),
                        targetResourceMapId, resourceMapIdsInDoc, aggregatedItemsInDoc, fieldNameRemoved);
                if (ids != null) {
                    for (String id : ids.keySet()) {
                        doc.removeFieldsWithValue(fieldNameRemoved, id);
                    }
                }
                doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP,
                        targetResourceMapId);
                updatedSolrDocs.add(doc);
                /*if (aggregatedItemsInDoc.size() > 1) {
                    

                } else {
                    //multiple resource map aggregate same metadata and data. Just remove the resource map
                    doc.removeFieldsWithValue(SolrElementField.FIELD_RESOURCEMAP,
                            targetResourceMapId);
                    updatedSolrDocs.add(doc);
                }*/
            }
        }
        return updatedSolrDocs;
    }

    /*
     * Return a map of mapping aggregation id map the target resourceMapId.
     * This will look the aggregation information in another side - If the targetId
     * is a metadata object, we will look the data objects which it describes; If 
     * the targetId is a data object, we will look the metadata object which documents it.
     */
    private Map<String, String> matchResourceMapsAndItems(String targetId,
            String targetResourceMapId, List<String> originalResourceMaps, List<String> aggregatedItems, String fieldName) {
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
                            //okay, we found the target aggregation item id and the resource map id
                            //in this solr doc. However, we need check if another resource map with different
                            //id but specify the same relationship. If we have the id(s), we should not
                            // remove the documents( or documentBy) element since we need to preserve the 
                            // relationship for the remain resource map. 
                            boolean hasDuplicateIds = false;
                            if(originalResourceMaps != null) {
                               for(String id :resourceMapIds) {
                                    if (originalResourceMaps.contains(id) && !id.equals(targetResourceMapId)) {
                                        hasDuplicateIds = true;
                                        break;
                                    }
                                }
                            }
                            if(!hasDuplicateIds) {
                                map.put(item, targetResourceMapId);
                            }
                            
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
    
    /*
     * Merge two list of updated solr docs. removedDocumentBy has the correct information about documentBy element.
     * removedDocuments has the correct information about the documents element.
     * So we go through the two list and found the two docs having the same identifier.
     * Get the list of the documents value from the one in the removedDoucments (1). 
     * Remove all values of documents from the one in the removedDocumentBy. 
     * Then copy the list of documents value from (1) to to the one in the removedDocumentBy.
     */
    private List<SolrDoc> mergeUpdatedSolrDocs(List<SolrDoc>removedDocumentBy, List<SolrDoc>removedDocuments) {
        List<SolrDoc> mergedDocuments = new ArrayList<SolrDoc>();
        if(removedDocumentBy == null || removedDocumentBy.isEmpty()) {
            mergedDocuments = removedDocuments;
        } else if (removedDocuments == null || removedDocuments.isEmpty()) {
            mergedDocuments = removedDocumentBy;
        } else {
            int sizeOfDocBy = removedDocumentBy.size();
            int sizeOfDocs = removedDocuments.size();
            for(int i=sizeOfDocBy-1; i>= 0; i--) {
                SolrDoc docInRemovedDocBy = removedDocumentBy.get(i);
                for(int j= sizeOfDocs-1; j>=0; j--) {
                    SolrDoc docInRemovedDocs = removedDocuments.get(j);
                    if(docInRemovedDocBy.getIdentifier().equals(docInRemovedDocs.getIdentifier())) {
                        //find the same doc in both list. let's merge them.
                        //first get all the documents element from the docWithDocs(it has the correct information about the documents element)
                        List<String> idsInDocuments = docInRemovedDocs.getAllFieldValues(SolrElementField.FIELD_DOCUMENTS);
                        docInRemovedDocBy.removeAllFields(SolrElementField.FIELD_DOCUMENTS);//clear out any documents element in docInRemovedDocBy
                        //add the Documents element from the docInRemovedDocs if it has any.
                        // The docInRemovedDocs has the correct information about the documentBy. Now it copied the correct information of the documents element.
                        // So docInRemovedDocs has both correct information about the documentBy and documents elements.
                        if(idsInDocuments != null) {
                            for(String id : idsInDocuments) {
                                if(id != null && !id.trim().equals("")) {
                                    docInRemovedDocBy.addField(new SolrElementField(SolrElementField.FIELD_DOCUMENTS, id));
                                }
                                
                            }
                        }
                        //intersect the resource map ids.
                        List<String> resourceMapIdsInWithDocs = docInRemovedDocs.getAllFieldValues(SolrElementField.FIELD_RESOURCEMAP);
                        List<String> resourceMapIdsInWithDocBy = docInRemovedDocBy.getAllFieldValues(SolrElementField.FIELD_RESOURCEMAP);
                        docInRemovedDocBy.removeAllFields(SolrElementField.FIELD_RESOURCEMAP);
                        Collection resourceMapIds = CollectionUtils.intersection(resourceMapIdsInWithDocs, resourceMapIdsInWithDocBy);
                        if(resourceMapIds != null) {
                            for(Object idObj : resourceMapIds) {
                                String id = (String)idObj;
                                docInRemovedDocBy.addField(new SolrElementField(SolrElementField.FIELD_RESOURCEMAP, id));
                            }
                        }
                        //we don't need do anything about the documentBy elements since the docInRemovedDocBy has the correct information.
                        mergedDocuments.add(docInRemovedDocBy);
                        //delete the two documents from the list
                        removedDocumentBy.remove(i);
                        removedDocuments.remove(j);
                        break;
                    }
                    
                }
            }
            // when we get there, if the two lists are empty, this will be a perfect merge. However, if something are left. we 
            //just put them in.
            for(SolrDoc doc: removedDocumentBy) {
                mergedDocuments.add(doc);
            }
            for(SolrDoc doc: removedDocuments) {
                mergedDocuments.add(doc);
            }
        }
        return mergedDocuments;
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
