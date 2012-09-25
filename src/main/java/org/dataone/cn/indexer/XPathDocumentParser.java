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

package org.dataone.cn.indexer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.indexer.parser.IDocumentSubprocessor;
import org.dataone.cn.indexer.parser.IPostProcessor;
import org.dataone.cn.indexer.parser.SolrField;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementAdd;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * User: Porter
 * Date: 7/25/11
 * Time: 1:28 PM
 */

/**
 * Main class for processing and updating solr index for DataOne.org solr
 * schema. See spring configuration file for more information.
 * 
 * There should only be one instance of XPathDocumentParser in place at a time
 * since it performs updates on the SOLR index and transactions on SOLR are at
 * the server level - so if multiple threads write and commit then things could
 * get ugly.
 * 
 */

public class XPathDocumentParser {
    public String index = null;
    private String solrBaseUri = null;
    private String solrindexUri = null;
    private String solrQueryUri = null;
    private List<SolrField> fields = null;
    /**
     * Document Sub Processors are executed after the fields have been processed
     * they are also allowed to add or replace existing data in the field list
     */
    private List<IDocumentSubprocessor> subprocessors = null;
    private XMLNamespaceConfig xmlNamespaceConfig = null;

    private static DocumentBuilderFactory documentBuilderFactory = null;
    private static DocumentBuilder builder = null;

    private static XPathFactory xpathFactory = null;
    private static XPath xpath = null;

    private static final String OUTPUT_ENCODING = "UTF-8";
    private static final String INPUT_ENCODING = "UTF-8";
    private HTTPService httpService = null;

    Log log = LogFactory.getLog(XPathDocumentParser.class);
    private List<IPostProcessor> postProcessors = new ArrayList<IPostProcessor>();

    static {
        documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        try {
            builder = documentBuilderFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        xpathFactory = XPathFactory.newInstance();
        xpath = xpathFactory.newXPath();
    }

    public XPathDocumentParser(XMLNamespaceConfig xmlNamespaceConfig, List<SolrField> fields)
            throws XPathExpressionException, ParserConfigurationException {
        this.xmlNamespaceConfig = xmlNamespaceConfig;
        this.fields = fields;
        init();
    }

    public void init() throws ParserConfigurationException, XPathExpressionException {
        xpath.setNamespaceContext(xmlNamespaceConfig);
        initExpressions();
    }

    private void initExpressions() throws XPathExpressionException {
        for (SolrField field : getFields()) {
            field.initExpression(xpath);
        }

    }

    long startTime = 0;

    // List<SolrDoc> docs = new ArrayList<SolrDoc>();

    /**
     * Given a PID, system metadata document path, and an optional document
     * path, populate the set of SOLR fields for the document and update the
     * index. Note that if the document is a resource map, then records that it
     * references will be updated as well.
     * 
     * @param id
     *            The object identifier
     * @param sysMetaPath
     *            File system path to a system metadata document
     * @param objectPath
     *            File system path to a science metadata document or resource
     *            map. This may be null if the object is a science data or other
     *            non-indexable type of object.
     * @return SolrDoc
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public SolrDoc processID(String id, String sysMetaPath, String objectPath) throws IOException,
            SAXException, ParserConfigurationException, XPathExpressionException, EncoderException {
        // Load the System Metadata document
        Document sysMetaDoc = loadDocument(sysMetaPath, INPUT_ENCODING);
        if (sysMetaDoc == null) {
            log.error("Could not load System metadata for ID: " + id);
            return null;
        }

        // Extract the field values from the System Metadata
        List<SolrElementField> sysSolrFields = processFields(sysMetaDoc, id);
        SolrDoc indexDocument = new SolrDoc(sysSolrFields);
        Map<String, SolrDoc> docs = new HashMap<String, SolrDoc>();
        docs.put(id, indexDocument);

        // Determine if subprocessors are available for this ID
        if (subprocessors != null) {
            // for each subprocessor loaded from the spring config
            for (IDocumentSubprocessor subprocessor : subprocessors) {
                // Does this subprocessor apply?
                if (subprocessor.canProcess(sysMetaDoc)) {
                    // if so, then extract the additional information from the
                    // document.
                    try {
                        // docObject = the resource map document or science
                        // metadata document.
                        // note that resource map processing touches all objects
                        // referenced by the resource map.
                        Document docObject = loadDocument(objectPath, INPUT_ENCODING);
                        if (docObject == null) {
                            log.error("Could not load OBJECT file for ID,Path=" + id + ", "
                                    + objectPath);
                        } else {
                            docs = subprocessor.processDocument(id, docs, docObject);
                        }
                    } catch (Exception e) {
                        log.error(e.getStackTrace().toString());
                    }
                }
            }
        }
        // ? why is this here
        // docs.put(id, indexDocument);

        // TODO: get list of unmerged documents and do single http request for
        // all
        // unmerged documents
        for (SolrDoc mergeDoc : docs.values()) {
            if (!mergeDoc.isMerged()) {
                mergeWithIndexedDocument(mergeDoc);
            }
        }

        SolrElementAdd addCommand = getAddCommand(new ArrayList<SolrDoc>(docs.values()));
        if (log.isTraceEnabled()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            addCommand.serialize(baos, OUTPUT_ENCODING);
            log.trace(baos.toString());
            // System.out.println(baos.toString());
        }
        sendCommand(addCommand);
        if (docs.size() > 0)
            docs.clear();

        return indexDocument;
    }

    public SolrDoc process(String id, InputStream systemMetaDataStream, String objectPath)
            throws IOException, SAXException, ParserConfigurationException,
            XPathExpressionException, EncoderException {

        // Load the System Metadata document
        Document sysMetaDoc = generateXmlDocument(systemMetaDataStream);
        if (sysMetaDoc == null) {
            log.error("Could not load System metadata for ID: " + id);
            return null;
        }

        // Extract the field values from the System Metadata
        List<SolrElementField> sysSolrFields = processFields(sysMetaDoc, id);
        SolrDoc indexDocument = new SolrDoc(sysSolrFields);
        Map<String, SolrDoc> docs = new HashMap<String, SolrDoc>();
        docs.put(id, indexDocument);

        // Determine if subprocessors are available for this ID
        if (subprocessors != null) {
            // for each subprocessor loaded from the spring config
            for (IDocumentSubprocessor subprocessor : subprocessors) {
                // Does this subprocessor apply?
                if (subprocessor.canProcess(sysMetaDoc)) {
                    // if so, then extract the additional information from the
                    // document.
                    try {
                        // docObject = the resource map document or science
                        // metadata document.
                        // note that resource map processing touches all objects
                        // referenced by the resource map.
                        Document docObject = loadDocument(objectPath, INPUT_ENCODING);
                        if (docObject == null) {
                            log.error("Could not load OBJECT file for ID,Path=" + id + ", "
                                    + objectPath);
                        } else {
                            docs = subprocessor.processDocument(id, docs, docObject);
                        }
                    } catch (Exception e) {
                        log.error(e.getStackTrace().toString());
                    }
                }
            }
        }

        // TODO: get list of unmerged documents and do single http request for
        // all
        // unmerged documents
        for (SolrDoc mergeDoc : docs.values()) {
            if (!mergeDoc.isMerged()) {
                mergeWithIndexedDocument(mergeDoc);
            }
        }

        SolrElementAdd addCommand = getAddCommand(new ArrayList<SolrDoc>(docs.values()));
        if (log.isTraceEnabled()) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            addCommand.serialize(baos, OUTPUT_ENCODING);
            log.trace(baos.toString());
        }

        sendCommand(addCommand);

        return indexDocument;
    }

    /**
     * Merge updates with existing solr documents
     * 
     * This method appears to re-set the data package field data into the
     * document about to be updated in the solr index. Since packaging
     * information is derived from the package document (resource map), this
     * information is not present when processing a document contained in a data
     * package. This method replaces those values from the existing solr index
     * record for the document being processed. -- sroseboo, 1-18-12
     * 
     * @param indexDocument
     * @return
     * @throws IOException
     * @throws EncoderException
     * @throws XPathExpressionException
     */
    // TODO:combine merge function with resourcemap merge function

    private SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument) throws IOException,
            EncoderException, XPathExpressionException {
        if (httpService == null) {
            return indexDocument;
        }
        List<String> ids = new ArrayList<String>();
        ids.add(indexDocument.getIdentifier());
        List<SolrDoc> indexedDocuments = httpService.getDocuments(solrQueryUri, ids);
        SolrDoc indexedDocument = indexedDocuments == null || indexedDocuments.size() <= 0 ? null
                : indexedDocuments.get(0);
        if (indexedDocument == null || indexedDocument.getFieldList().size() <= 0) {
            return indexDocument;
        } else {
            for (SolrElementField field : indexedDocument.getFieldList()) {
                if ((field.getName().equals(SolrElementField.FIELD_ISDOCUMENTEDBY)
                        || field.getName().equals(SolrElementField.FIELD_DOCUMENTS) || field
                        .getName().equals(SolrElementField.FIELD_RESOURCEMAP))
                        && !indexDocument.hasFieldWithValue(field.getName(), field.getValue())) {
                    indexDocument.addField(field);
                }
            }

            indexDocument.setMerged(true);
            return indexDocument;
        }
    }

    private void sendCommand(SolrElementAdd addCommand) throws IOException {
        HTTPService service = getHttpService();

        try {
            service.sendUpdate(getSolrindexUri(), addCommand, OUTPUT_ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SolrElementAdd getAddCommand(List<SolrDoc> docs) {
        return new SolrElementAdd(docs);
    }

    private List<SolrElementField> processFields(Document doc, String identifier) {

        List<SolrElementField> fieldList = new ArrayList<SolrElementField>();
        // solrFields is the list of fields defined in the application context
        // bean config file
        List<SolrField> solrFields = getFields();
        //
        for (SolrField field : solrFields) {
            try {
                // the field.getFields method can return a single value or
                // multiple values for multi-valued fields
                // or can return multiple SOLR document fields.
                fieldList.addAll(field.getFields(doc, identifier));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return fieldList;

    }

    public Document loadDocument(String filePath) throws ParserConfigurationException, IOException,
            SAXException {
        return loadDocument(filePath, INPUT_ENCODING);
    }

    private Document loadDocument(String filePath, String input_encoding)
            throws ParserConfigurationException, IOException, SAXException {
        Document doc = null;
        FileInputStream fis = null;
        InputStreamReader isr = null;
        try {
            fis = new FileInputStream(filePath);
            isr = new InputStreamReader(fis, input_encoding);
            InputSource source = new InputSource(isr);
            doc = builder.parse(source);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error parsing file: " + filePath);
        } finally {
            if (isr != null) {
                isr.close();
            }
            if (fis != null) {
                fis.close();
            }
        }
        return doc;
    }

    public Document generateXmlDocument(InputStream smdStream) throws SAXException {
        Document doc = null;

        try {
            doc = builder.parse(smdStream);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return doc;
    }

    public XMLNamespaceConfig getXmlNamespaceConfig() {
        return xmlNamespaceConfig;
    }

    public void setXmlNamespaceConfig(XMLNamespaceConfig xmlNamespaceConfig) {
        this.xmlNamespaceConfig = xmlNamespaceConfig;
    }

    public List<SolrField> getFields() {
        return fields;
    }

    public void setFields(List<SolrField> fields) {
        this.fields = fields;
    }

    public String getSolrindexUri() {
        return solrindexUri;
    }

    public void setSolrindexUri(String solrindexUri) {
        this.solrindexUri = solrindexUri;
    }

    public void setHttpService(HTTPService service) {
        this.httpService = service;
    }

    public HTTPService getHttpService() {
        return httpService;
    }

    public String getSolrQueryUri() {
        return solrQueryUri;
    }

    public void setSolrQueryUri(String solrQueryUri) {
        this.solrQueryUri = solrQueryUri;
    }

    public void setSolrBaseUri(String solrBaseUri) {
        this.solrBaseUri = solrBaseUri;
        setSolrQueryUri(solrBaseUri + "/adminSelect/");
        setSolrindexUri(solrBaseUri + "/update?commit=true");
    }

    public String getSolrBaseUri() {
        return solrBaseUri;
    }

    public List<IDocumentSubprocessor> getSubprocessors() {
        return subprocessors;
    }

    public void setSubprocessors(List<IDocumentSubprocessor> subprocessorList) {
        for (IDocumentSubprocessor subprocessor : subprocessorList) {
            subprocessor.initExpression(xpath);
        }
        this.subprocessors = subprocessorList;
    }

    public static DocumentBuilder getDocumentBuilder() {
        return builder;
    }

    public void setPostProcessors(List<IPostProcessor> postProcessors) {
        this.postProcessors = postProcessors;
    }

}
