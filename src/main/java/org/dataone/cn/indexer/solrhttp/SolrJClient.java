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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.service.util.DateTimeMarshaller;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Adaptation of HttpService class to use the SolrJClient.
 * The SolrClient query responses return a list of SolrDocuments which
 * are adapted to our SolrDoc class.  (All fields in SolrDoc get represented
 * as Strings, instead of Objects.
 * 
 * @author rnahf
 */

public class SolrJClient implements D1IndexerSolrClient {

    private static final String CHAR_ENCODING = "UTF-8";
    private static final String XML_CONTENT_TYPE = "text/xml";

    final static String PARAM_START = "start";
    final static String PARAM_ROWS = "rows";
    final static String PARAM_INDENT = "indent";
    final static String VALUE_INDENT_ON = "on";
    final static String VALUE_INDENT_OFF = "off";
    final static String PARAM_QUERY = "q";

    private static final String MAX_ROWS = "5000";
    
    private static final String DYNAMIC_FIELD_SUFFIX = "_sm";
    
    /** >0 value ensures a soft commit after that number of millis
     * 0 value is an immediate soft commit
     * -1 value switches client behavior to add a hard commit from the client
     */
    public int COMMIT_WITHIN_MS = 0; // for solr updates.
    
    public static final boolean USE_REAL_TIME_GETS = false;

    private static Logger log = Logger.getLogger(SolrJClient.class.getName());

    private HttpClient httpClient;
    private SolrClient solrClient;


    private String SOLR_SCHEMA_PATH;
    private String solrIndexUri;
    private List<String> validSolrFieldNames = new ArrayList<String>();
    private Set<String> multiValuedSolrFieldNames = new HashSet<String>();
    
    protected List<String> solrCallList = new ArrayList<String>();
    protected List<Date> solrCallStartTimeList = new ArrayList<Date>();
    protected List<Long> solrCallDurationList = new ArrayList<Long>();


    /**
     * @param SolrClient
     */
    public SolrJClient(SolrClient client) {
        solrClient = client;
    }
    


    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#sendUpdate(java.lang.String, org.dataone.cn.indexer.solrhttp.SolrElementAdd)
     */
    @Override
    public void sendUpdate(String uri, List<SolrDoc> data) throws IOException {
        sendUpdate(uri, data, CHAR_ENCODING);
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#sendUpdate(java.lang.String, org.dataone.cn.indexer.solrhttp.SolrElementAdd, java.lang.String, java.lang.String)
     */

    public void sendUpdate(String uri, List<SolrDoc> data, String encoding)
            throws IOException {

        sendUpdate(uri, data, encoding, true);
    }
        
    /**
     * performs an update 
     * @param uri
     * @param data
     * @param encoding
     * @param isAtomic - if atomic, add in field modifiers
     * @throws IOException
     */
    @Override
    public void sendUpdate(String uri, List<SolrDoc> data, String encoding, boolean isAtomic)
            throws IOException {

        log.debug("isAtomic: " + isAtomic);
        // convert SolrElementAdd to SolrJ's SolrInputDocument
        List<SolrInputDocument> updateDocList = new ArrayList<SolrInputDocument>();
        for (SolrDoc doc : data) {
            SolrInputDocument updateDoc = new SolrInputDocument();
            log.debug("new doc to update...");
            boolean hasAtomicUpdates = false;
            for (SolrElementField sef : doc.getFieldList()) {
                
                if (sef.getModifier() != null) 
                {
                    hasAtomicUpdates = true;
                    Map<String, Object> fieldModifier = new HashMap<>(1);
                    fieldModifier.put(sef.getModifier().toString(), sef.getValue());
                    updateDoc.addField(sef.getName(), fieldModifier);
                    log.debug(String.format("update field '%s' as '%s' with value '%s'", sef.getName(), sef.getModifier(), sef.getValue()));
                } 
                else if (isAtomic && !sef.getName().equals("id") && !sef.getName().equals("_version_")) 
                {
                    hasAtomicUpdates = true;
                    Map<String,Object> fieldModifier = new HashMap<>(1);
                    String modifierKeyword = this.multiValuedSolrFieldNames.contains(sef.getName()) ? "add" : "set";
                    fieldModifier.put(modifierKeyword, sef.getValue());
                    updateDoc.addField(sef.getName(), fieldModifier);
                    log.debug(String.format("update field '%s' as '%s' with value '%s'", sef.getName(), modifierKeyword, sef.getValue()));
                } 
                else 
                {
                    updateDoc.addField(sef.getName(), sef.getValue());
                    log.debug(String.format("update field '%s' with value '%s'", sef.getName(), sef.getValue()));
                    
                }
            }
            if (!isAtomic || hasAtomicUpdates) {
                updateDocList.add(updateDoc);                
            } 
        }

        if (updateDocList.size() > 0) {
            Date callStart = null;
            long callEnd = 0;
            try {
                log.info("submitting update for " + updateDocList.size() + " documents.");
                callStart = new Date();
                if (COMMIT_WITHIN_MS == -1) {
                    getSolrClient().add(updateDocList);
                    getSolrClient().commit();
                } else {
                    getSolrClient().add(updateDocList,COMMIT_WITHIN_MS);
                }
                callEnd = System.currentTimeMillis();
                log.info(".... update submitted");
            } catch (SolrServerException e) {
                logError(e,data,e.getMessage(),"Using zkHost");
                throw new IOException("Unable to update solr (see cause)",e);
            } finally {
                this.solrCallList.add("update:" + updateDocList.size());
                this.solrCallDurationList.add(callEnd - callStart.getTime());
                this.solrCallStartTimeList.add(callStart);
            }
        }
    }


    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#sendSolrDelete(java.lang.String)
     */
    @Override
    public void sendSolrDelete(String pid) {
          
        try {
            log.info("Deleting record in Solr with id: " + pid);
            if (COMMIT_WITHIN_MS == -1) {
                getSolrClient().deleteById(pid);
                getSolrClient().commit();
            } else {
                getSolrClient().deleteById(pid,COMMIT_WITHIN_MS);
            }
        } catch (SolrServerException | IOException e1) {
            logError(e1,pid, e1.getMessage(),"zkHost");
            e1.printStackTrace();
        }
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#sendSolrDeletes(java.util.List)
     */
    @Override
    public void sendSolrDeletes(List<String> pids) {
             
        try {
            log.info("Deleting records in Solr with id: " + String.join(",  ",pids));
            if (COMMIT_WITHIN_MS == -1) {
                getSolrClient().deleteById(pids);
                getSolrClient().commit();
            } else {
                getSolrClient().deleteById(pids,COMMIT_WITHIN_MS);
            }

        } catch (SolrServerException | IOException e1) {
            String pidString = StringUtils.join(pids, ",");
            logError(e1,pidString, e1.getMessage(),"zkHost");
            e1.printStackTrace();
        }
     }
    
    private void logError(Exception ex, List<SolrDoc> data, String messageResponse,
            String uri) throws IOException {

        try {
            if (ex != null) {
                log.error("Unable to write to stream", ex);
            }
            SolrElementAdd add = new SolrElementAdd(data);
            log.error("URL: " + uri);
            log.error("Post: ");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            add.serialize(baos, "UTF-8");
            log.error(new String(baos.toByteArray(), "UTF-8"));
            log.error("\n\n\nResponse: \n");

            log.error(messageResponse);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void logError(Exception ex, String data, String messageResponse, String uri)
    {

        try {
            if (ex != null) {
                log.error("Unable to write to stream", ex);
            }

            log.error("URL: " + uri);
            log.error("Post: ");
            log.error(data);
            log.error("\n\n\nResponse: \n");
            
            log.error(messageResponse);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getDocumentsById(java.lang.String, java.util.List)
     */
    @Override
    public List<SolrDoc> getDocumentsByD1Identifier(String uir, List<String> ids) throws IOException
    {
        List<SolrDoc> docs = getDocumentsBySolrId(ids);
    
        if (docs.size() < ids.size()) 
            docs.addAll(getDocumentsByField(uir, ids, SolrElementField.FIELD_SERIES_ID, false));
    
        return docs;
    }

    /** 
     * wrapper for the solrClient getById() method that provides 'real-time get' to uncommitted updates. 
     * (this is not a normal query)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getDocumentById(java.lang.String, java.lang.String)
     */
    @Override
    public List<SolrDoc> getDocumentBySolrId(String uir, String id) throws IOException
    {
        if (USE_REAL_TIME_GETS) {
            try {
                log.info("Id for solrId get: " + id);
                if (log.isTraceEnabled()) {
                    log.trace(Thread.currentThread().getStackTrace()[1].getMethodName());
                    log.trace(Thread.currentThread().getStackTrace()[2].getMethodName());
                    log.trace(Thread.currentThread().getStackTrace()[3].getClassName() + ":"+ Thread.currentThread().getStackTrace()[5].getMethodName());
                    log.trace(Thread.currentThread().getStackTrace()[4].getClassName() + ":"+ Thread.currentThread().getStackTrace()[6].getMethodName());            
                }

                SolrDocument d = this.getSolrClient().getById(id);

                List<SolrDoc> docs = new ArrayList<SolrDoc>();
                docs.add(parseResponse(d));
                return docs;

            } catch (SolrServerException e) {
                throw new IOException(e);
            }

        } else {
            return getDocumentsByField(uir, Collections.singletonList(id), SolrElementField.FIELD_ID, false);
        }
    }
    /**
     * wrapper for the solrClient getById() that takes a list.  This method provides 'real-time get' to uncommitted updates.
     * @param ids
     * @return
     * @throws IOException
     */
    public List<SolrDoc> getDocumentsBySolrId(List<String> ids) throws IOException
    {
        if (USE_REAL_TIME_GETS) {
            try {
                log.info("Ids for solrId get: " + String.join(", ",ids));
                if (log.isTraceEnabled()) {
                    log.trace(Thread.currentThread().getStackTrace()[1].getMethodName());
                    log.trace(Thread.currentThread().getStackTrace()[2].getMethodName());
                    log.trace(Thread.currentThread().getStackTrace()[3].getClassName() + ":"+ Thread.currentThread().getStackTrace()[5].getMethodName());
                    log.trace(Thread.currentThread().getStackTrace()[4].getClassName() + ":"+ Thread.currentThread().getStackTrace()[6].getMethodName());       
                }
                SolrDocumentList d = this.getSolrClient().getById(ids);
                List<SolrDoc> docs = new ArrayList<SolrDoc>();
                docs.addAll(parseResponse(d));
                return docs;

            } catch (SolrServerException e) {
                throw new IOException(e);
            }
        } else {
            return getDocumentsByField(null, ids, SolrElementField.FIELD_ID, false);
        }
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getDocumentsByResourceMap(java.lang.String, java.lang.String)
     */
    @Override
    public List<SolrDoc> getDocumentsByResourceMap(String uir, String resourceMapId) throws IOException 
    {
        return getDocumentsByField(uir, Collections.singletonList(resourceMapId),
                SolrElementField.FIELD_RESOURCEMAP, true);
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getDocumentsByField(java.lang.String, java.util.List, java.lang.String, boolean)
     */
    @Override
    public List<SolrDoc> getDocumentsByField(String uir, List<String> fieldValues,
            String queryField, boolean maxRows) throws IOException 
            {

        if (fieldValues == null || fieldValues.size() <= 0) {
            return null;
        }

        loadSolrSchemaFields();

        List<SolrDoc> docs = new ArrayList<SolrDoc>();

        int rows = 0;
        String rowString = "";
        StringBuilder sb = new StringBuilder();
        for (String id : fieldValues) {
            if (!StringUtils.isEmpty(id)) {
                if (sb.length() > 0) {
                    sb.append(" OR ");
                }
                sb.append(queryField + ":").append(ClientUtils.escapeQueryChars(id));
                rows++;
                if (sb.length() > 5000) {
                    if (maxRows) {
                        rowString = MAX_ROWS;
                    } else {
                        rowString = Integer.toString(rows);
                    }
                    docs.addAll(doRequest(uir, sb, rowString));
                    rows = 0;
                    sb = new StringBuilder();
                }
            }
        }
        if (sb.length() > 0) {
            if (maxRows) {
                rowString = MAX_ROWS;
            } else {
                rowString = Integer.toString(rows);
            }
            docs.addAll(doRequest(uir, sb, rowString));
        }
        return docs;
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getDocumentsByResourceMapFieldAndDocumentsField(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public List<SolrDoc> getDocumentsByResourceMapFieldAndDocumentsField(String uir,
            String resourceMapId, String documentsId) throws IOException {
        
        return getDocumentsByTwoFields(uir, SolrElementField.FIELD_RESOURCEMAP, resourceMapId,
                SolrElementField.FIELD_DOCUMENTS, documentsId);
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getDocumentsByResourceMapFieldAndIsDocumentedByField(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public List<SolrDoc> getDocumentsByResourceMapFieldAndIsDocumentedByField(String uir,
            String resourceMapId, String isDocumentedById) throws IOException
    {

        return getDocumentsByTwoFields(uir, SolrElementField.FIELD_RESOURCEMAP, resourceMapId,
                SolrElementField.FIELD_ISDOCUMENTEDBY, isDocumentedById);
    }

    private List<SolrDoc> getDocumentsByTwoFields(String uir, String field1, String field1Value,
            String field2, String field2Value) throws IOException {

        loadSolrSchemaFields();
        List<SolrDoc> docs = new ArrayList<SolrDoc>();
        StringBuilder sb = new StringBuilder();
        sb.append(field1 + ":").append(ClientUtils.escapeQueryChars(field1Value));
        sb.append(" AND ");
        sb.append(field2 + ":").append(ClientUtils.escapeQueryChars(field2Value));
        docs.addAll(doRequest(uir, sb, MAX_ROWS));
        return docs;
    }

    
    /**
     * executes and parses the query response.
     * Does not escape the query 
     * @param uir
     * @param sb
     * @param rows
     * @return
     * @throws IOException
     */
    protected List<SolrDoc> doRequest(String uir, StringBuilder sb, String rows) throws IOException 
    {
        String solrQ = sb.toString();// ClientUtils.escapeQueryChars(sb.toString());
        
        log.info(solrQ);
        if (log.isTraceEnabled()) {
            log.trace(this);
            log.trace(Thread.currentThread().getStackTrace()[3].getMethodName());
            log.trace(Thread.currentThread().getStackTrace()[4].getMethodName());
            log.trace(Thread.currentThread().getStackTrace()[5].getClassName() + ":"+ Thread.currentThread().getStackTrace()[5].getMethodName());
            log.trace(Thread.currentThread().getStackTrace()[6].getClassName() + ":"+ Thread.currentThread().getStackTrace()[6].getMethodName());    
        }
        SolrQuery sq = new SolrQuery();

        sq.setQuery(solrQ);
        sq.setParam(PARAM_INDENT, VALUE_INDENT_ON);
        if (rows != null) {
            sq.setParam(PARAM_START, "0");
            sq.setParam(PARAM_ROWS, ClientUtils.escapeQueryChars(rows)); // probably not needed, but who knows what will be passed in?
        }

        Date callStart = new Date();
        long callEnd = 0;
        try {
            QueryResponse qr = getSolrClient().query(sq);

            List<SolrDoc> response =  parseResponse(qr.getResults());
            callEnd = System.currentTimeMillis();
            return response;
            
        } catch (SolrServerException e1) {
            throw new IOException(e1);
        } finally {
            this.solrCallList.add(solrQ);
            this.solrCallDurationList.add(callEnd - callStart.getTime());
            this.solrCallStartTimeList.add(callStart);
        }
    }
    
    
    
    protected List<SolrDoc> parseResponse(SolrDocumentList sdl) {
        
        Iterator<SolrDocument> it = sdl.iterator();
        int countDoc = 0;
        List<SolrDoc> docs = new ArrayList<SolrDoc>();
        while (it.hasNext()) {
            countDoc++;
            docs.add(parseResponse(it.next()));
        }
        log.info("DocCount = " + countDoc);
        return docs;
    
    }
    
    protected SolrDoc parseResponse(SolrDocument sd) {
                       
        loadSolrSchemaFields();

        SolrDoc solrDoc = new SolrDoc();
        
        if (sd == null) {
            return null;
        }
        if (sd.hasChildDocuments()) {
            log.info("ChildDocCount = " + sd.getChildDocumentCount());
        }
        int fieldCount = 0;
        StringBuffer sb = new StringBuffer();
        for (String fieldName : sd.getFieldNames()) {
            fieldCount++;
            sb.append(" [" + fieldName + ": ");

            // ensure valid field, or a dynamic field matching the pattern
            // NOTE: there are many kinds of dynamic fields, only handling one for now.
            if (this.validSolrFieldNames.contains(fieldName) || fieldName.endsWith(DYNAMIC_FIELD_SUFFIX)) {
                sb.append("valid: ");
                Object v = sd.getFieldValue(fieldName);

                if (v instanceof Collection) {
                    sb.append("multi-valued: ");
                    for (Object vv : sd.getFieldValues(fieldName)) {
                        SolrElementField sef = new SolrElementField();
                        sef.setName(fieldName);
                        sef.setValue(valueConverter(vv));
                        solrDoc.addField(sef);
                        sb.append(valueConverter(vv) + "'");
                    }
                } else {
                    SolrElementField sef = new SolrElementField();
                    sef.setName(fieldName);
                    sef.setValue(valueConverter(v));
                    solrDoc.addField(sef);
                    sb.append(valueConverter(v));
                }  
            } else {
                sb.append("NOT valid");
            }
            sb.append("]");
            if (log.isTraceEnabled()) {
                log.trace(sb.toString());
                
            }
        }
        log.info("FieldCount = " + fieldCount);
        return solrDoc;
    }
    
    
    
    private String valueConverter(Object o) {
        try {
            return (String) o;
        } catch (ClassCastException e) {
            if (o instanceof Long) {
                return Long.toString((Long) o);
            } else if (o instanceof Boolean) {
                return Boolean.toString((Boolean) o);
            } else if (o instanceof Date) {
                String date = DateTimeMarshaller.serializeDateToUTC((Date) o);
                return StringUtils.replace(date, "+00:00", "Z");
                
            } else if (o instanceof Float) {
                return Float.toString((Float) o);
            } else if (o instanceof Integer) {
                return Integer.toString((Integer) o);
            }
            throw e;
        }
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#retrieveDocumentFromSolrServer(java.lang.String, java.lang.String)
     */
    @Override
    public SolrDoc retrieveDocumentFromSolrServer(String id, String solrQueryUri) throws IOException
    {
        List<String> ids = new ArrayList<String>();
        ids.add(id);
        List<SolrDoc> indexedDocuments = getDocumentsByD1Identifier(solrQueryUri, ids);
        if (indexedDocuments.size() > 0) {
            return indexedDocuments.get(0);
        } else {
            return null;
        }
    }


    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#setSolrSchemaPath(java.lang.String)
     */
    @Override
    public void setSolrSchemaPath(String path) {
        SOLR_SCHEMA_PATH = path;
    }

    private void loadSolrSchemaFields() {
        if (SOLR_SCHEMA_PATH != null && validSolrFieldNames.isEmpty()) {
            Document doc = loadSolrSchemaDocument();
            NodeList nList = doc.getElementsByTagName("copyField");
            List<String> copyDestinationFields = new ArrayList<String>();
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);
                String destinationField = node.getAttributes().getNamedItem("dest").getNodeValue();
                copyDestinationFields.add(destinationField);
            }
            nList = doc.getElementsByTagName("field");
            List<String> fields = new ArrayList<String>();
            for (int i = 0; i < nList.getLength(); i++) {
                Node node = nList.item(i);
                
                String fieldName = node.getAttributes().getNamedItem("name").getNodeValue();
                fields.add(fieldName);
                
                // create a list of multivalued fields to be used for atomic updates
                Node n = node.getAttributes().getNamedItem("multiValued");
                if (n != null && "true".equals(n.getNodeValue())) {
                    this.multiValuedSolrFieldNames.add(fieldName);
                }
                
            }
            fields.removeAll(copyDestinationFields);
            this.validSolrFieldNames = fields;
            fields.remove("_version_");
        }
    }

    private Document loadSolrSchemaDocument() {

        Document doc = null;
        File schemaFile = new File(SOLR_SCHEMA_PATH);
        if (schemaFile != null) {
            FileInputStream fis = null;
            try {
                fis = new FileInputStream(schemaFile);
            } catch (FileNotFoundException e) {
                log.error(e.getMessage(), e);
            }
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = null;
            try {
                dBuilder = dbFactory.newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                log.error(e.getMessage(), e);
            }
            try {
                doc = dBuilder.parse(fis);
            } catch (SAXException e) {
                log.error(e.getMessage(), e);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        return doc;
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#setSolrIndexUri(java.lang.String)
     */
    @Override
    public void setSolrIndexUri(String uri) {
        this.solrIndexUri = uri;
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getSolrIndexUri()
     */
    @Override
    public String getSolrIndexUri() {
        return this.solrIndexUri;
    }

    /* (non-Javadoc)
     * @see org.dataone.cn.indexer.solrhttp.D1IndexerSolrClient#getHttpClient()
     */
    @Override
    public HttpClient getHttpClient() {
        return this.httpClient;     }
    
    protected SolrClient getSolrClient() {
        return solrClient;

    }
}
