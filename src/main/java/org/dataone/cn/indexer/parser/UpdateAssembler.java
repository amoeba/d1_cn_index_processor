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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.cn.indexer.solrhttp.SolrElementField.Modifier;
import org.dataone.cn.indexer.solrhttp.SolrSchema;


/**
 * With partial updates and creation of stub records, all index tasks
 * need to query solr for existing records.  This class keeps track of
 * the queried documents and manages creation of the partial update by comparision
 * with the existing solr document. If a version conflict happens in the client,
 * the UpdateAssembler can recover by replacing the existing solr record 
 * with a newer one and rebuilding the partial update.
 * Addition of new  material from subprocessors is kept separate to allow
 * multiple updates to the same solr record to be properly combined 
 * before creating the update that's submitted to the solr client.
 * 
 * An instance of this class is NOT meant to be shared across threads.
 * 
 * @author rnahf
 *
 */
public class UpdateAssembler {
    
    private static Logger _log = Logger.getLogger(UpdateAssembler.class);
    
//    private final static SolrDoc notInSolr;
//
//    static {
//        notInSolr = new SolrDoc();
//        notInSolr.addField(new SolrElementField("_version","-1"));
//    }
    
    private HashMap<String,Long> currentVersion = new HashMap<>();
    private HashMap<String,SolrDoc> existingMap = new HashMap<>();
    private TreeMap<String,List<SolrDoc>> updates = new TreeMap<>(); // needs to come out sorted
    
    
    private SolrSchema solrSchema;
    
    public UpdateAssembler(SolrSchema solrSchema) {
        this.solrSchema = solrSchema;
    }
    

//    public static SolrDoc getNotInSolrSolrDoc() {
//        return notInSolr;
//    }

    
    /**
     * Adds the new material to a list of updates to overlay upon the existing solr record
     * @param id
     * @param existing
     * @param newStuff
     */
    public void addToUpdate(String id, SolrDoc existing, SolrDoc newMaterial) {
        if (existing != null ) {
            String versionString = existing.getFirstFieldValue("_version_");
            if (versionString != null) {
                Long version = Long.valueOf(versionString); 
           
                if (currentVersion.containsKey(id) && currentVersion.get(id) > version) {
                    // reject the new 'existing' because it's out of date
                } else {
                    currentVersion.put(id, version);
                    existingMap.put(id,existing);
                }
            } else {
                String message = "_version_ field is somehow null.  If in production, look at" + 
                        " the schema and SolrSchema class and SolrJClient#parseResponse for problems";
                _log.error("for id " + id + ": "+ message);
                throw new RuntimeException(message);
            }
        }
        if (newMaterial != null) {
            if (!updates.containsKey(id)) {
                updates.put(id, new ArrayList<SolrDoc>());
            }
            updates.get(id).add(newMaterial);
        }
    }

    
    /**
     * Returns a sorted list of SolrDocs beginning at the startIndex.
     * The list is a new List instance, but of possibly existing SolrDocs
     * @param startIndex - used for cases of version conflict of a batch update
     * @return
     */
    public List<SolrDoc> assembleUpdate(int startIndex) {
        List<SolrDoc> assembledUpdate = new ArrayList<>();
        if (_log.isDebugEnabled()) {
            _log.debug("entering assembleUpdate.(startIndex = " + startIndex + " / endIndex = " 
                    + (updates.keySet().size()-1) + ")");
        }
        // consolidates the newMaterial into a new SolrDoc
        int i = 0;
        for(Entry<String,List<SolrDoc>> n : updates.entrySet()) {
            String id = n.getKey();
            if (i >= startIndex) {
                if (_log.isDebugEnabled()) {
                    _log.debug(String.format("Consolidating for id %s, from existing version %s, with %d updates",
                            id, this.currentVersion.get(id), n.getValue().size()));
                }
            
                SolrDoc collectingDoc = null;
                for(SolrDoc update : n.getValue()) {
                    collectingDoc = consolidateNewMaterial(id, collectingDoc, update);
                }
                SolrDoc partialUpdate = createPartialUpdate(existingMap.get(id), collectingDoc);
                if (partialUpdate != null) {
                    assembledUpdate.add(partialUpdate);
                }
            }
            i++;
        }
        return assembledUpdate;
    }
    
    /**
     * Acts upon the existing hashmap to incorporate the updates
     * Results in the desired future solr record
     * (No partial update logic is applied)
     * @param id
     * @param consolidationTarget - the document to collect newMaterial into, can be null
     * @param newMaterial- the new SolrDoc to merge into the collectingDoc
     * @return the consolidated SolrDoc
     */
    protected SolrDoc consolidateNewMaterial(String id, SolrDoc consolidationTarget, SolrDoc newMaterial) {
        
        // determine the document to consolidate into
        if (consolidationTarget == null) {
            
            // try to use the existing document
            if (existingMap.get(id) != null) {
                consolidationTarget = existingMap.get(id).clone();
            
            } else {
                // use the newMaterial, and simply return, because nothing to be done 
                if (newMaterial != null) {
                    return newMaterial.clone();
                
                } else {
                    return null; 
                }
            }
        }         
        if (_log.isDebugEnabled()) {
            _log.debug("  (the consolidation target for id " + id + " has "
                    + consolidationTarget.getFieldList().size() + " elements.)");
        }
        // Each segment handles consolidation differently
        for (String segment: this.solrSchema.listSegments()) {
            switch(segment) {
            case "sysmeta" :
            case "scimeta":
            case "mn_service":    
                
                // values in these segments replace other values
                // so the entire segment gets wiped, then newMaterial is added

                if (getSegment(newMaterial, segment) != null
                        && !getSegment(newMaterial, segment).isEmpty()) {

                    // remove all segment values from existing
                    for (String fieldname : solrSchema.getAllSegmentFields(segment)) {
                        consolidationTarget.removeAllFields(fieldname);
                    }
                    // add all segment values from 
                    for (SolrElementField sef: getSegment(newMaterial, segment)) {
                        consolidationTarget.addField(sef);
                    }
                }
                break;

            case "ore":
            case "prov":
            case "sem":
                
                // values in these segements either replace or are added to existing values
                // depending on whether it's multi-valued or not
                
                if (getSegment(newMaterial, segment) != null
                        && !getSegment(newMaterial, segment).isEmpty()) {

                    // compare with existing and add new fields
                    for (SolrElementField field: getSegment(newMaterial, segment)) {

                        if (solrSchema.isFieldMultiValued(field.getName())) {
                            // add if not already present
                            if (!consolidationTarget.containsElement(field)) {
                                consolidationTarget.addField(field); 
                            }

                        } else {
                            // replace because it's a single-valued field
                            consolidationTarget.removeAllFields(field.getName());
                            consolidationTarget.addField(field);                                
                        }
                    }
                }
                break;
            case "internal":
                //  do nothing
                break;
            default:
                // TODO:  throw exception??
            }
        }    
        return consolidationTarget;
    } 
    
    
    /**
     * creates a partial update from the presumed existing record
     * and the desired future record
     * @param existing
     * @param future
     * @return
     * @throws Exception
     */
    private SolrDoc createPartialUpdate(SolrDoc existing, SolrDoc future) { 
        SolrDoc update = new SolrDoc();
        _log.debug("entering createPartialUpdate...");
        // if there's no existing document, then the future is the update
        // but we still have to determine the concurrency approach
        if (existing == null && future != null) {
            _log.debug("No existing solr record found");
            update = future.clone();
            update.updateOrAddField("_version_","-1");
        } else {
            // loop through each schema field and figure out what to do
            // single-valued fields either get set or removed
            // multi-valued fields are either added to or removed
            // (different segments have different remove behavior)
            
            boolean hasSysmetaSegment = false;
            for (String fieldName : solrSchema.getValidFields()) {
                //if (schema.getFieldSegment(fieldName).equals("internal") 
                if (fieldName.equals("id") || fieldName.equals("_version_")) {
                    continue;
                }
                
                if (solrSchema.isFieldMultiValued(fieldName)) {
                    ////////////  multi-valued  ///////////////
                   
                    // certain segments replace the list instead of adding to them
                    // in these cases, we need to create REMOVE statements for
                    // existing values that are not remaining
                   String segment = solrSchema.getFieldSegment(fieldName);
                   _log.trace("segment is: " + segment + " for field: " + fieldName );
                   boolean removeExistingFirst = (segment.equals("sysmeta") 
                           || segment.equals("scimeta") 
                           || segment.equals("mn_service"));
                   
                   if (existing.hasField(fieldName)) {
                       if (future.hasField(fieldName)) {
                           ////// need to add new values, and possibly remove existing values
                           ////// (don't act on values that don't change)
                           
                           ////// first determine unique fields
                           List<SolrElementField> eValues = existing.getFields(fieldName);
                           List<SolrElementField> fValues = future.getFields(fieldName);
                           @SuppressWarnings("unchecked")
                           Collection<SolrElementField> both = CollectionUtils.intersection(eValues, fValues);
                           eValues.removeAll(both);  // now unique to existing
                           fValues.removeAll(both);  // now unique to future
                           
                           if (removeExistingFirst) {
                               // remove existing if unique
                               for (SolrElementField e : eValues) {
                                   SolrElementField clone = e.clone();
                                   clone.setModifier(SolrElementField.Modifier.REMOVE);
                                   update.addField(clone);
                               }
                           }
                               
                           // add unique future values
                           for (SolrElementField f : fValues) {
                               SolrElementField clone = f.clone();
                               clone.setModifier(SolrElementField.Modifier.ADD);
                               update.addField(clone);                           
                           }
                       } else {
                           // remove all existing
                           if (removeExistingFirst) {
                               // remove existing if unique
                               for (SolrElementField e : existing.getFields(fieldName)) {
                                   SolrElementField clone = e.clone();
                                   clone.setModifier(SolrElementField.Modifier.REMOVE);
                                   update.addField(clone);
                               }
                           }
                       }
                   } else {
                       if (future.hasField(fieldName)) {
                           // add all
                           for (SolrElementField f : future.getFields(fieldName)) {
                               SolrElementField clone = f.clone();
                               clone.setModifier(SolrElementField.Modifier.ADD);
                               update.addField(clone);                           
                           }
                       } else {
                           ; // no-op  (both null)
                       }
                   }
                    
                } else {
                    ////////////  single-valued   ////////////////
                    if (existing.hasField(fieldName)) {
                        if (future.hasField(fieldName)) {
                            // compare values
                            // while we could overwrite if the values are the same, 
                            // it might trigger an unnecessary index
                            if (!existing.getFirstFieldValue(fieldName).equals(future.getFirstFieldValue(fieldName))) {
                                SolrElementField clone = future.getField(fieldName);
                                clone.setModifier(SolrElementField.Modifier.SET);
                                update.addField(clone);
                            } else {
                                ; //no-op
                            }
                        } else {
                            // remove existing
                            SolrElementField clone = existing.getField(fieldName).clone();
                            clone.setModifier(SolrElementField.Modifier.REMOVE);
                            update.addField(clone);
                        }
                    } else {
                        if (future.hasField(fieldName)) {
                            //set future value
                            SolrElementField clone = future.getField(fieldName);
                            clone.setModifier(SolrElementField.Modifier.SET);
                            update.addField(clone);
                        } else {
                            ; // no-op, 
                        }
                    }
                }
            
            }
            // Only populate the id and version fields if theres something to update
            // (it's possible that the update matches what's already existing, in which case
            // we want to return a null document)
            if (update.getFieldList().size() > 0) {
                update.addField(existing.getField("_version_"));
                update.addField(existing.getField("id"));
            } else {
                return null;
            }
        }
        // without the ADD-DISTINCT modifier (introduced in V7x), we can't try to 
        // add stubs without checking the existing, so if we don't have existing,
        // we will need to enforce the version -1 constraint then deal with the 
        // consequences
        // (the following block will not be used until then).
        //_log.debug(".createPartialUpdate/check for stub-only");
        String version = update.getFirstFieldValue("_version_"); 
        if (version == null) {
            _log.debug(".createPartialUpdate:  version = " + version);
            // in this situation, there either wasn't an existing document (create)
            // or it was never queried for (relationship stubs)
            // (or version wasn't returned with the solr record)

            List<SolrElementField> sysmeta = getSegment(update, "sysmeta");
            _log.debug("sysmeta field count: " + (sysmeta != null ? sysmeta.size() : "null"));
            List<SolrElementField> scimeta = getSegment(update, "scimeta");
            _log.debug("scimeta field count: " + (scimeta != null ? scimeta.size() : "null"));
            List<SolrElementField> service = getSegment(update, "mn_service");
            _log.debug("mn_service field count: " + (service != null ? service.size() : "null"));
            
            if (getSegment(update, "sysmeta").isEmpty()
                    && getSegment(update, "scimeta").isEmpty()
                    && getSegment(update, "mn_service").isEmpty()) {
                
                _log.debug(".createPartialUpdate / looks like a stub...");
                // 
                // we can assume that the update is ore, prov, or semantics
                // and can use add-distinct semantics
                for(SolrElementField sef: update.getFieldList()) {
                    if (_log.isDebugEnabled()) {
                        _log.debug("fieldName: " + sef.getName() + "; value: "+ sef.getValue());
                    }
                    if (sef.getName().equals("id")) {
                        continue;
////// already did the check on version field in outer conditional
//                    } else if (sef.getName().equals("_version_")) {
//                        sef.setValue("0");
//                        // (version doesn't matter)
//                        
                    } else {
                        
 //                       sef.setModifier(Modifier.ADD_DISTINCT);
                        sef.setModifier(Modifier.ADD);
                    }
                }       
            } else {
                _log.debug(".createPartialUpdate / not a stub");
            }
        } 
        if (_log.isDebugEnabled()) {
            _log.debug("createPartialUpdate complete..." );
        
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                update.serialize(baos, "UTF-8");
                _log.debug(baos.toString());
            } catch (IOException e) {
                _log.debug("!!!! Not able to serialize the update for debugging...: " + e.getMessage());
            } finally {
                IOUtils.closeQuietly(baos);
            }
        }
       
        return update;       
    }
    
    
    /**
     * gets all of the fields for a given segment
     * @param segmentName
     * @return
     */
    public List<SolrElementField> getSegment(SolrDoc doc, String segmentName) {
        List<SolrElementField> fieldsToReturn = new ArrayList<>();
        
        _log.debug("entering getSegment(..) ...");
        if (doc != null && doc.getFieldList() != null) {
            _log.trace("getting all segment fields from schema for segment: '"+ segmentName + "', schema: " + solrSchema);
            List<String> segFields = solrSchema.getAllSegmentFields(segmentName);
            _log.trace("got segment fields for " + segmentName);
            for (SolrElementField field : doc.getFieldList()) {               
                if (segFields != null && segFields.contains(field.getName())) {
                    fieldsToReturn.add(field);
                }
            }
        }
        _log.trace("...leaving getSegment()");
        return fieldsToReturn;
    }
}
