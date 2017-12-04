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

package org.dataone.cn.indexer.parser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseDocumentDeleteSubprocessor implements IDocumentDeleteSubprocessor {

    private static Logger log = Logger.getLogger(BaseDocumentDeleteSubprocessor.class);
    private String instanceLabel;
    
    @Autowired
    private D1IndexerSolrClient d1IndexerSolrClient;

    @Autowired
    private String solrQueryUri;

    
    
    private String relationSourceFormatId;
    private String relationSourceField;
    private List<String> biDirectionalRelationFields;
    private List<String> uniDirectionalRelationFields;
    
    boolean usingAtomicUpdates = true;

    public BaseDocumentDeleteSubprocessor() {
    }

    /*
     * (non-Javadoc)
     * @see org.dataone.cn.indexer.parser.IDocumentDeleteSubprocessor#processDocForDelete(java.lang.String, java.util.Map)
     */
    public Map<String, SolrDoc> processDocForDelete(String identifier, Map<String, SolrDoc> docMap)
            throws Exception {

        SolrDoc indexedDoc = d1IndexerSolrClient.retrieveDocumentFromSolrServer(identifier, solrQueryUri);
        if (indexedDoc != null) {
            if (hasRelationsBySource(indexedDoc)) {
                // it's an object that has related fields in it from the source object
                docMap.putAll(removeBiDirectionalRelationsForDoc(identifier, indexedDoc, docMap));
            }
            if (isRelationshipSource(indexedDoc)) {
                // it's a source object that contributes field values to other documents
                docMap.putAll(removeRelationsBySourceDoc(identifier, indexedDoc, docMap));
            }
        }
        if (log.isDebugEnabled()) {
            if (docMap != null) {
                log.debug("... docs returned for [" + StringUtils.join(docMap.keySet(),", ") + "]");
            } else {
                log.debug("... docs returned for [null]");
            }
        }
        return docMap;
    }


    /**
     * returns true if the indexedDoc's object format field value matches the format of the subprocessor's relationSourceFormatId field
     * @param indexedDoc
     * @return
     * @throws Exception
     */
    private boolean isRelationshipSource(SolrDoc indexedDoc) throws Exception {
        String formatId = indexedDoc.getFirstFieldValue(SolrElementField.FIELD_OBJECTFORMAT);
        return relationSourceFormatId.equals(formatId);
    }

    /* 
     * removes relationship fields from the solr documents that have them
     *  
     */
    private Map<String, SolrDoc> removeRelationsBySourceDoc(String relationSourceId,
            SolrDoc indexedDoc, Map<String, SolrDoc> docs) throws Exception {

        // gather all docs with relations from self in the relation source field  (for example: 'resourceMap' field)
        List<SolrDoc> relatedDocs = d1IndexerSolrClient.getDocumentsByField(solrQueryUri,
                Collections.singletonList(relationSourceId), relationSourceField, true);

        Set<String> otherSourceDocs = new HashSet<String>();
        
        // removes the relationship fields from the related documents
        // regardless of source. hmm....
        for (SolrDoc relatedDoc : relatedDocs) {

            // gather other relation source docs from modified list
            otherSourceDocs.addAll(relatedDoc.getAllFieldValues(relationSourceField));

            // remove relation fields (uni and bi-directional)
            // add modified docs to update list
            String relatedDocId = relatedDoc.getFirstFieldValue(SolrElementField.FIELD_ID);
            if (docs.get(relatedDocId) != null) {
                // replace the relatedDoc that's being worked on with the one passed into the method
                relatedDoc = docs.get(relatedDocId);
            }


            if (usingAtomicUpdates) {
                log.debug("...processing using atomic updates");
                // build a diffDoc that instructs solr to delete these fields from the record
                List<SolrElementField> fields = new ArrayList<>();
                fields.add(relatedDoc.getField(SolrElementField.FIELD_ID));

                SolrElementField sef = new SolrElementField(relationSourceField, null);
                sef.setModifier(SolrElementField.Modifier.SET);
                fields.add(sef);
                for (String relationField : getBiDirectionalRelationFields()) {
                    SolrElementField s = new SolrElementField(relationField,null);
                    s.setModifier(SolrElementField.Modifier.SET); // setting to null is how to remove all
                    fields.add(s);
                }

                for (String relationField : getUniDirectionalRelationFields()) {
                    SolrElementField s = new SolrElementField(relationField,null);
                    s.setModifier(SolrElementField.Modifier.SET); // setting to null is how to remove all
                    fields.add(s);
                }
                SolrDoc sd = new SolrDoc();
                sd.setFieldList(fields);
                docs.put(relatedDocId, sd);

            }
            else {
                log.debug("...processing using non-atomic updates");
                relatedDoc.removeAllFields(relationSourceField);

                for (String relationField : getBiDirectionalRelationFields()) {
                    relatedDoc.removeAllFields(relationField);
                }
                for (String relationField : getUniDirectionalRelationFields()) {
                    relatedDoc.removeAllFields(relationField);
                }
                docs.put(relatedDocId, relatedDoc);
                log.debug("... ... putting doc in map " + relatedDocId);
            }

        }
        // the null value for the netry signals the caller that these other documents need to be reindexed
        // (as per the interface)
        for (String otherRelatedDoc : otherSourceDocs) {
            if (!otherRelatedDoc.equals(relationSourceId)) {
                docs.put(otherRelatedDoc, null);
            }
        }
        return docs;
    }
    

 

    
    /**
     * The prototypical situation for this method is when a solr field is defined that contains the source
     * of other relationship statements in the solr document.  For example, the 'resourceMap' field.
     * 
     * @param indexedDoc - the solr document being examined
     * @return - returns true if the subprocessor instance defines a relationSourceField value, and that field value in the indexedDoc is not null
     * @throws XPathExpressionException
     * @throws IOException
     * @throws EncoderException
     */
    private boolean hasRelationsBySource(SolrDoc indexedDoc) throws XPathExpressionException,
            IOException, EncoderException {
        String relationSourceId = indexedDoc.getFirstFieldValue(relationSourceField);
        return StringUtils.isNotEmpty(relationSourceId);
    }
    
    
    
    
    private Map<String, SolrDoc> removeBiDirectionalRelationsForDoc(String identifier,
            SolrDoc indexedDoc, Map<String, SolrDoc> docMap) throws Exception {

        // related docs are those whose ids are the values of the relationship fields used in the search query
        
        for (String relationField : getBiDirectionalRelationFields()) {
            List<SolrDoc> relatedDocs = d1IndexerSolrClient.getDocumentsByField(solrQueryUri,
                    Collections.singletonList(identifier), relationField, true);
            for (SolrDoc retrievedRelatedDoc : relatedDocs) {
                String relatedDocId = retrievedRelatedDoc.getFirstFieldValue(SolrElementField.FIELD_ID);
                
                // always work with the relatedDoc from the map if it exists there
                SolrDoc chosenRelatedDoc = docMap.get(relatedDocId) != null ? docMap.get(relatedDocId) : retrievedRelatedDoc;

                if (usingAtomicUpdates) {
                    log.debug("...processing using atomic updates");
                    
                    // build a diffDoc that instructs solr to delete these fields from the record
                    List<SolrElementField> fields = new ArrayList<>();
 
                    // 1. make sure we have the id field               
                    fields.add(chosenRelatedDoc.getField(SolrElementField.FIELD_ID));
                    
                    
                    // 2. remove the identifier from the related field if it's there
                    SolrElementField sef = new SolrElementField(relationField, identifier);
                    sef.setModifier(SolrElementField.Modifier.REMOVE);
                    fields.add(sef);
                    
                    
                    SolrDoc sd = new SolrDoc();
                    sd.setFieldList(fields);
                    docMap.put(relatedDocId, sd);
                    log.debug("... ... putting doc in map " + relatedDocId);
                    
                } else {
                    log.debug("...processing using non-atomic updates");
                    
                    
                    chosenRelatedDoc.removeFieldsWithValue(relationField, identifier);
                    docMap.put(relatedDocId, chosenRelatedDoc);
                    log.debug("... ... putting doc in map " + relatedDocId);
                }
            }

        }
        return docMap;
    }
    
    
    

    private List<String> getBiDirectionalRelationFields() {
        if (biDirectionalRelationFields == null) {
            biDirectionalRelationFields = new ArrayList<String>();
        }
        return biDirectionalRelationFields;
    }

    private List<String> getUniDirectionalRelationFields() {
        if (uniDirectionalRelationFields == null) {
            uniDirectionalRelationFields = new ArrayList<String>();
        }
        return uniDirectionalRelationFields;
    }

    public String getRelationSourceFormatId() {
        return relationSourceFormatId;
    }

    public void setRelationSourceFormatId(String relationSourceFormatId) {
        this.relationSourceFormatId = relationSourceFormatId;
    }

    public String getRelationSourceField() {
        return relationSourceField;
    }

    public void setRelationSourceField(String relationSourceField) {
        this.relationSourceField = relationSourceField;
    }

    public void setBiDirectionalRelationFields(List<String> biDirectionalRelationFields) {
        this.biDirectionalRelationFields = biDirectionalRelationFields;
    }

    public void setUniDirectionalRelationFields(List<String> uniDirectionalRelationFields) {
        this.uniDirectionalRelationFields = uniDirectionalRelationFields;
    }

    public void setInstanceLabel(String instanceLabel) {
        this.instanceLabel = instanceLabel;
    }
    
    @Override
    public String getInstanceLabel() {
        return instanceLabel;
    }
}
