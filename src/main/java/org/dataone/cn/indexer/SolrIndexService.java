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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.parser.IDocumentSubprocessor;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementAdd;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.xml.sax.SAXException;

/**
 * Top level document processing class.  
 * 
 * Contains collection of document sub-processors which are used to mine search
 * index data from document objects.  Each sub-processor is configured via spring
 * to collect data from different types of documents (by formatId).
 * 
 * There should only be one instance of XPathDocumentParser in place at a time
 * since it performs updates on the SOLR index and transactions on SOLR are at
 * the server level - so if multiple threads write and commit then things could
 * get messy.
 * 
 * User: Porter
 * Date: 7/25/11
 * Time: 1:28 PM
 */

public class SolrIndexService {

    private static Logger log = Logger.getLogger(SolrIndexService.class);

    private String solrindexUri = null;
    private String solrQueryUri = null;

    /**
     * Document Sub Processors are executed after the fields have been processed
     * they are also allowed to add or replace existing data in the field list
     */
    private List<IDocumentSubprocessor> subprocessors = null;
    private IDocumentSubprocessor systemMetadataProcessor = null;

    private static final String OUTPUT_ENCODING = "UTF-8";

    private HTTPService httpService = null;

    long startTime = 0;

    public SolrIndexService() {
    }

    /**
     * Given a PID, system metadata input stream, and an optional document
     * path, populate the set of SOLR fields for the document and update the
     * index. Note that if the document is a resource map, then records that it
     * references will be updated as well.
     * 
     * @param id
     * @param systemMetaDataStream
     * @param objectPath
     * @return
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws XPathExpressionException
     * @throws EncoderException
     */
    public void insertIntoIndex(String id, InputStream systemMetaDataStream, String objectPath)
            throws IOException, SAXException, ParserConfigurationException,
            XPathExpressionException, EncoderException {

        Map<String, SolrDoc> docs = new HashMap<String, SolrDoc>();
        try {
            docs = systemMetadataProcessor.processDocument(id, docs, systemMetaDataStream);
        } catch (Exception e) {
            log.error("Error parsing system metadata for id: " + id + e.getMessage());
            e.printStackTrace();
        }
        String formatId = docs.get(id).getFirstFieldValue(SolrElementField.FIELD_OBJECTFORMAT);

        for (IDocumentSubprocessor subprocessor : getSubprocessors()) {
            if (subprocessor.canProcess(formatId)) {
                try {
                    // note that resource map processing touches all objects
                    // referenced by the resource map.
                    FileInputStream objectStream = new FileInputStream(objectPath);
                    if (!objectStream.getFD().valid()) {
                        log.error("Could not load OBJECT file for ID,Path=" + id + ", "
                                + objectPath);
                    } else {
                        docs = subprocessor.processDocument(id, docs, objectStream);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
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
        List<SolrDoc> indexedDocuments = httpService.getDocumentsById(solrQueryUri, ids);
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

    public String getSolrindexUri() {
        return solrindexUri;
    }

    public void setSolrIndexUri(String solrindexUri) {
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

    public List<IDocumentSubprocessor> getSubprocessors() {
        if (this.subprocessors == null) {
            this.subprocessors = new ArrayList<IDocumentSubprocessor>();
        }
        return subprocessors;
    }

    public void setSubprocessors(List<IDocumentSubprocessor> subprocessorList) {
        this.subprocessors = subprocessorList;
    }

    public IDocumentSubprocessor getSystemMetadataProcessor() {
        return systemMetadataProcessor;
    }

    public void setSystemMetadataProcessor(IDocumentSubprocessor systemMetadataProcessor) {
        this.systemMetadataProcessor = systemMetadataProcessor;
    }
}
