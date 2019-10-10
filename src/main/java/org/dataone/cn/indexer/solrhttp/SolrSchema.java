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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.hp.hpl.jena.sparql.util.CollectionUtils;

/**
 * A class to encapsulate the index schema details
 * so as to allow manipulation of SolrDocs
 * 
 * configured in     cp:application-context-solr-schema.xml
 * test config'd in  o.d.c.index.test-solr-schema.xml
 * 
 * meant to reused a single instance via spring bean framework
 * 
 * @author rnahf
 *
 */
public class SolrSchema {

    private String solrSchemaPath;
    
    private Map<String,ValueType> validSolrFieldNames = new HashMap<>(); // initialize with map to avoid NPEs
    private Set<String> multiValuedSolrFieldNames = new HashSet<>();
    
    private Map<String,String> fieldSegmentMap = new HashMap<>();
    private Map<String,List<String>> segmentFieldsMap = new HashMap<>();
    

    private static Logger log = Logger.getLogger(SolrSchema.class.getName());
    
    /*
     * known types used by the index schema
     * These will be used for converting strings to the proper type
     * @author rnahf
     *
     */
    enum ValueType {
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        DATE,
        BOOLEAN,
        STRING
    }
    
    
    public SolrSchema() {
    }

    /**
     * setter for bean configuration
     * @param path
     * @throws IOException 
     * @throws SAXException 
     * @throws ParserConfigurationException 
     */
    public void setSolrSchemaPath(String path) throws ParserConfigurationException, SAXException, IOException {
        this.solrSchemaPath = path;
        loadSolrSchemaFields();
    }
    
    /** 
     * setter for bean configuration
     * @param fieldSegmentMap
     */
    public void setFieldSegmentMap(Map<String,String> fieldSegmentMap) {
        this.fieldSegmentMap = fieldSegmentMap;
        log.debug("entering SolrSchema.setFieldSegmentMap( ) with parameter instance " + fieldSegmentMap);
        for (Map.Entry<String, String> n : this.fieldSegmentMap.entrySet()) {
            String field = n.getKey();
            String segment = n.getValue();
            if (!this.segmentFieldsMap.containsKey(segment)) {
                this.segmentFieldsMap.put(segment, new ArrayList<String>());
            }
            this.segmentFieldsMap.get(segment).add(field);
        }
    }
   
    
    /**
     * returns the set of valid fields extracted from the solr schema file
     * @return
     */
    public Set<String> getValidFields() {
        return this.validSolrFieldNames.keySet();
    }
    
    /**
     * returns the solr field's value-type (using solr types) 
     * @param name
     * @return
     */
    public ValueType getFieldType(String name) {
        return this.validSolrFieldNames.get(name);
    }
    
    /**
     * as determined from the solr schema
     * @param fieldName
     * @return
     */
    public boolean isFieldMultiValued(String fieldName) {
        return multiValuedSolrFieldNames.contains(fieldName);
    }
 
    
//    public boolean isFieldInSegment(String field, String segment) {
//        if (segment == null || field == null) {
//            return false;
//        }
//        return segment.equals(this.fieldSegmentMap.get(field));
//    }
    
    /** 
     * gets the assigned segment for the given field
     * 
     * @param field
     * @return
     */
    public String getFieldSegment(String field) {
        return this.fieldSegmentMap.get(field);
    }
    
    /**
     * as determined from the fieldSegmentMap
     * @param segment
     * @return
     */
    public List<String> getAllSegmentFields(String segment) {
        log.trace("getAllSegmentFields(); map is " + this.segmentFieldsMap); 
        return this.segmentFieldsMap.get(segment);
    }
    
    
    public Set<String> listSegments() {
        return this.segmentFieldsMap.keySet();
    }
    
    /**
     * From a string value for a solr field, convert it to the Java-type
     * listed in the solr schema.
     * 
     * @param fieldName
     * @param value
     * @return
     */
    public Object convertToSolrType(String fieldName, String value) {
        SolrSchema.ValueType type = this.validSolrFieldNames.get(fieldName);
        switch (type) {
            case LONG:
                return  Long.valueOf(value);
            case INTEGER:
                return  Integer.valueOf(value);
            case DATE:
                return org.dataone.service.util.DateTimeMarshaller.deserializeDateToUTC(value);
            case DOUBLE:
                return  Double.valueOf(value);
            case FLOAT:
                return  Float.valueOf(value);
            case BOOLEAN:
                return  Boolean.valueOf(value);
            default:
                return value;  // default to string
        }
    }
    
    /*
     * used when parsing the incoming document to map the schema types
     * to coded types that will be used for SolrInputDocuments
     * (see convertToSolrType)
     */
    private ValueType parseSolrValueType(String type) {
        ValueType mappedType;
        switch (type) {
        case "int":
        case "tint":
            mappedType = ValueType.INTEGER;
            break;
        case "long":
        case "tlong":
            mappedType = ValueType.LONG;
            break;
        case "double":
        case "tdouble":
            mappedType = ValueType.DOUBLE;
            break;
        case "float":
        case "tfloat":
            mappedType = ValueType.FLOAT;
            break;
        case "boolean":
            mappedType = ValueType.BOOLEAN;
            break;
        case "date":
        case "tdate":
            mappedType = ValueType.DATE;
            break;
        case "string":
        case "text_general":
        case "text_en_splitting":    
            mappedType = ValueType.STRING;
            break;
            
        default:
            mappedType = ValueType.STRING;
            
        }
        return mappedType;
    }
    
    
    // typically only called once per instance
    private void loadSolrSchemaFields() throws ParserConfigurationException, SAXException, IOException {
        log.debug("entering SolrSchema.loadSolrSchemaFields()...path: " + solrSchemaPath);
        if (solrSchemaPath != null && validSolrFieldNames.isEmpty()) {
            Document doc = loadSolrSchemaDocument();
            NodeList nList = doc.getElementsByTagName("copyField");
            List<String> copyDestinationFields = new ArrayList<String>();
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);
                String destinationField = node.getAttributes().getNamedItem("dest").getNodeValue();
                log.debug(" **** copy destination field: " + destinationField);
                copyDestinationFields.add(destinationField);
            }
            nList = doc.getElementsByTagName("field");
            Map<String,ValueType> fields = new HashMap<>();
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);
                
                String fieldName = node.getAttributes().getNamedItem("name").getNodeValue();
                ValueType fieldType = parseSolrValueType(node.getAttributes().getNamedItem("type").getNodeValue());
                fields.put(fieldName,fieldType);
                log.debug("valid field name: " + fieldName + " type: " + fieldType);
                // create a list of multivalued fields to be used for atomic updates
                Node n = node.getAttributes().getNamedItem("multiValued");
                if (n != null && "true".equals(n.getNodeValue())) {
                    this.multiValuedSolrFieldNames.add(fieldName);
                }
                
            }
            for (String cpf : copyDestinationFields) {
                fields.remove(cpf);
            }
            this.validSolrFieldNames = fields;
        }
    }
    
    
    private Document loadSolrSchemaDocument() throws ParserConfigurationException, SAXException, IOException {

        log.info("loading schema document from path: "+ solrSchemaPath);

        FileInputStream fis = null;
        try {
            fis =  new FileInputStream(new File(solrSchemaPath));

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fis);
            return doc;
        
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }
    
}
