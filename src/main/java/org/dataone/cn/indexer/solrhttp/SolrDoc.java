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

package org.dataone.cn.indexer.solrhttp;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.jena.atlas.logging.Log;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * A list representation of a solr record where each field value
 * is represented as a list element (tuple).  Multi-valued solr fields
 * are represented as multiple elements in the list with the same name.
 * 
 * Although defined in the SolrElementField class, this class provides
 * methods for element retrieval by different tuple properties.  
 * 
 * @see SolrElementField for details about the tuple.
 */
public class SolrDoc {
    public static final char[] ELEMENT_DOC_OPEN = "<doc>".toCharArray();
    public static final char[] ELEMENT_DOC_CLOSE = "</doc>".toCharArray();
    private static final String DYNAMIC_FIELD_SUFFIX = "_sm";
    

    
    private List<SolrElementField> fieldList = new ArrayList<SolrElementField>();
    private FieldCounter fieldCounts = new FieldCounter();
    
    
    class FieldCounter {
        
        HashMap<String,MutableInt> counters;
        
        FieldCounter() {
            counters = new HashMap<>();
        }
        
        public int getCount(String name) {
            if (counters.containsKey(name)) {
                return counters.get(name).intValue();
            } else {
                return 0;
            }
        }
        
        public int increment(String name) {
            return increment(name,1);
        }
        
        
        public int increment(String name, int howMany) {
            if (counters.containsKey(name)) {
                counters.get(name).add(howMany);
            } else {
                counters.put(name, new MutableInt(howMany));
            }
            return counters.get(name).intValue();
        }
        
        
        public int decrement(String name) {
            return decrement(name,1);
        }
        
        
        /**
         * decrements to a lower limit of zero (0),
         *  
         * @param name
         * @return the number decremented
         */
        public int decrement(String name, int howMany) {
            int count = 0;
            if (counters.containsKey(name)) {
               count = counters.get(name).intValue();
            } else {
                return 0;
            }
            
            if (howMany <= count) {
               counters.get(name).subtract(howMany);
               return howMany;
            } else {
                counters.get(name).setValue(0);
                return count;
            }
        }       
    }
    
    // private Boolean resourceMap = null;
    private String identifier;
    private String seriesId;

    private boolean merged = false;

    
    /**
     * Constructor to use when building the SolrDoc with addField method
     */
    public SolrDoc() {

    }

    /**
     * Creates a SolrDoc initialized with the given fieldList
     * @param fieldList
     */
    public SolrDoc(List<SolrElementField> fieldList) {
        this.fieldList = fieldList;
        fieldCounts = new FieldCounter();
        for (SolrElementField sef: fieldList) {
            fieldCounts.increment(sef.getName());
        }
    }
    
    /** 
     * Builds a deep-copy clone of a SolrDoc.
     */
    public SolrDoc clone() {
        SolrDoc clone = new SolrDoc();
        for(SolrElementField sef : this.fieldList) {
            clone.addField(sef.clone());
        }
        return clone;
    }
    
    
    /**
     * Creates a SolrDoc initialized with items from the given dom.Element
     *
     * @param docElement the DOM to parse into SolrDoc
     * @param validFields - if defined, filters out fields with unknown names.
     */
    public SolrDoc(Element docElement, Set<String> validFields) {
        this.fieldList = new ArrayList<SolrElementField>();
        NodeList values = docElement.getChildNodes();
        for (int i = 0; i < values.getLength(); i++) {
            Node n = (Node) values.item(i);
            if (n instanceof Text) {
                continue;
            }
            Element elementField = (Element) n;
            String tagName = elementField.getTagName();
            String fieldName = elementField.getAttribute("name");
            if (validFields != null) {
                // ensure valid field, or a dynamic field matching the pattern
                // NOTE: there are many kinds of dynamic fields, only handling one for now.
                if (!validFields.contains(fieldName) && !fieldName.endsWith(DYNAMIC_FIELD_SUFFIX)) {
                    continue;
                }
            }
            if (tagName.equals("arr")) {
                NodeList arrayValues = elementField.getChildNodes();
                for (int j = 0; j < arrayValues.getLength(); j++) {
                    Node nv = arrayValues.item(j);
                    if (nv instanceof Text) {
                        continue;
                    }
                    Node arrayValue = (Node) nv;
                    String valueString = arrayValue.getTextContent();
                    fieldList.add(new SolrElementField(fieldName, valueString));
                    fieldCounts.increment(fieldName);
                }
            } else {
                // If not adding from an arr element, then be sure only to add
                // one
                // instance of the field.
                if (!hasField(fieldName)) {
                    String valueString = elementField.getTextContent();
                    fieldList.add(new SolrElementField(fieldName, valueString));
                    fieldCounts.increment(fieldName);
                }
            }
        }
    }
    

    /**
     * @return the list of SolrElementFields in this document, 
     * multivalued fields are represented as multiple items in
     * the list with the same fieldName.     * 
     */
    public List<SolrElementField> getFieldList() {
        return fieldList;
    }
    
    /**
     * 
     * @return
     */
    public Map<String,MutableInt> getFieldCounter() {
        return fieldCounts.counters;
        
    }

//    /**
//     * method for bean creation?  Not sure this is useful.
//     * @param fieldList
//     */
//    public void setFieldList(List<SolrElementField> fieldList) {
//        this.fieldList = fieldList;
//        fieldNames = new HashSet<>();
//        for (SolrElementField sef: fieldList) {
//            fieldNames.add(sef.getName());
//        }
//    }

    public void serialize(OutputStream outputStream, String encoding) throws IOException {

        IOUtils.write(ELEMENT_DOC_OPEN, outputStream, encoding);

        for (SolrElementField field : getFieldList()) {
            field.serialize(outputStream, encoding);
        }
        IOUtils.write(ELEMENT_DOC_CLOSE, outputStream, encoding);

    }

    /**
     * tests for the presence or absence of a field in the field list
     * @param fieldName
     * @return
     */
    public boolean hasField(String fieldName) {
        return fieldCounts.getCount(fieldName) > 0;
    }

    /**
     * A simple lookup of the solr document's field value for comparison with the given string.
     * @param fieldName
     * @param value
     * @return
     */
    public boolean hasFieldWithValue(String fieldName, String value) {
        if (hasField(fieldName)) {
            for (SolrElementField field : fieldList) {
                if (field.getName() != null && field.getName().equals(fieldName)) {
                    if (field.getValue().equals(value)) {
                        return true;
                    }   
                }   
            }   
        }
        return false;
    }

    /**
     * returns the first item matching the fieldName
     * @param fieldName
     * @return SolrElementField 
     */
    public SolrElementField getField(String fieldName) {
        SolrElementField searchField = null;
        if (hasField(fieldName)) {
            for (SolrElementField field : fieldList) { 
                if (field.getName().equals(fieldName)) {
                    searchField = field;
                    break;
                }
            }
        }
        return searchField;
    }

 

    public String getIdentifier() {
        if (identifier == null) {
            identifier = getFirstFieldValue(SolrElementField.FIELD_ID);
        }
        return identifier;
    }

    public String getSeriesId() {
        if (seriesId == null) {
            seriesId = getFirstFieldValue(SolrElementField.FIELD_SERIES_ID);
        }
        return seriesId;
    }

    /**
     * returns true if the SolrElementField is in the SolrDoc
     * based, on SolrElementField equality (name, value property equivalence only) 
     * @param field
     * @return
     */
    public boolean containsElement(SolrElementField field) {
        return getFieldList().contains(field);
    }
    
    /**
     * Adds a SolrElementField to the list of fields
     * @param field
     */
    public void addField(SolrElementField field) {
        getFieldList().add(field);
        fieldCounts.increment(field.getName());
    }

    /**
     * Removes the first encountered field with the same name
     * @param fieldToRemove
     */
    public void removeField(String fieldToRemove) {
        if (fieldToRemove == null) {
            return;
        }
        SolrElementField temp = null;
        for (SolrElementField field : fieldList) {
            if (field.getName().equals(fieldToRemove)) {
                temp = field;
                break;
            }
        }
        fieldList.remove(temp);
        fieldCounts.decrement(temp.getName());
    }

    public List<SolrElementField> getFields(String fieldName) {
        List<SolrElementField> fieldsToReturn = new ArrayList<>();
        for (SolrElementField field : fieldList) {
            if (field.getName().equals(fieldName)) {
                fieldsToReturn.add(field);
            }
        }
        return fieldsToReturn;
    }
    
//    /**
//     * gets all of the fields for a given segment
//     * @param segmentName
//     * @return
//     */
//    public List<SolrElementField> getSegment(SolrSchema solrSchema, String segmentName) {
//        List<SolrElementField> fieldsToReturn = new ArrayList<>();
//        if (segmentName != null && this.fieldList != null) {
//            List<String> segFields = solrSchema.getAllSegmentFields(segmentName);
//            System.out.println("SorDoc.getSegment() number of fieldNames for segment: " +segmentName + " = " + (segFields == null ? "null" : segFields.size()));
//            for (SolrElementField field : this.fieldList) {               
//                if (segFields != null && segFields.contains(field.getName())) {
//                    fieldsToReturn.add(field);
//                }
//            }
//        }
//        return fieldsToReturn;
//    }
    
    
    
    /**
     * Removes all fields with the given name
     * @param fieldName
     */
    public void removeAllFields(String fieldName) {
        List<SolrElementField> fieldsToRemove = getFields(fieldName);
        fieldList.removeAll(fieldsToRemove);
        fieldCounts.decrement(fieldName, fieldsToRemove.size());
    }

    
    
    /**
     * removes the SolrElementField matching the name and value parameters from the SolrDoc.  Note that
     * multivalued fields are represented as multiple SolrElementFields, so this operation effectively
     * removes only the one item from the list of values when transcribed into a SolrDocument via update. 
     * 
     * @param name
     * @param value
     */
    public void removeFieldsWithValue(String name, String value) {
        List<SolrElementField> fieldsToRemove = new ArrayList<>();
        for (SolrElementField field : fieldList) {
            if (field.getName().equals(name) && field.getValue().equals(value)) {
                fieldsToRemove.add(field);
            }
        }
        fieldList.removeAll(fieldsToRemove);
        fieldCounts.decrement(name, fieldsToRemove.size());
    }

    public void removeOneFieldWithValue(String name, String value) {
        List<SolrElementField> fieldsToRemove = new ArrayList<>();
        for (SolrElementField field : fieldList) {
            if (field.getName().equals(name) && field.getValue().equals(value)) {
                fieldsToRemove.add(field);
                break;
            }
        }
        fieldList.removeAll(fieldsToRemove);
        fieldCounts.decrement(name, fieldsToRemove.size());
    }

    /**
     * 
     * @param fieldName
     * @return returns first value for given field name
     */
    public String getFirstFieldValue(String fieldName) {
        for (SolrElementField field : fieldList) {
            if (field.getName().equals(fieldName)) {
                return field.getValue();
            }
        }
        return null;

    }

    public List<String> getAllFieldValues(String fieldName) {
        List<String> allValues = new ArrayList<String>();
        for (SolrElementField field : fieldList) {
            if (field.getName().equals(fieldName)) {
                allValues.add(field.getValue());
            }
        }
        return allValues;
    }

    /**
     * sets the value for a given field, overwriting if there was a previous value
     * @param fieldName
     * @param fieldValue
     */
    public void updateOrAddField(String fieldName, String fieldValue) {
        for (SolrElementField solrElementField : fieldList) {
            if (solrElementField.getName().equals(fieldName)) {
                solrElementField.setValue(fieldValue);
                return;
            }
        }
        SolrElementField field = new SolrElementField(fieldName, fieldValue);
        fieldList.add(field);
        fieldCounts.increment(fieldName);
    }

    public boolean isMerged() {
        return merged;
    }

    public void setMerged(boolean merged) {
        this.merged = merged;
    }

    /**
     * A document is visible in the index if systemMetdata.archived is 
     * not set to true (null or false)
     * @param smd - a systemmetadata object
     * @return - false if smd is null or archived.  true otherwise
     */
    public static boolean visibleInIndex(SystemMetadata smd) {
        
        if (smd == null)                return false; // can't assume it's visible if it's null...
        if (smd.getArchived() == null)  return true;  // null = no archived, so visible
        if (smd.getArchived())          return false; // archived means not visible
        return true;                                  // fall-through case is not archived, so visible

        //        return !(smd.getArchived() != null && smd.getArchived().booleanValue());
    }
}
    