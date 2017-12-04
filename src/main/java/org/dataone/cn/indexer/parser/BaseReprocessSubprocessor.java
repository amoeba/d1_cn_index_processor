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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.client.v2.formats.ObjectFormatCache;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.generator.IndexTaskGenerator;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.parser.utility.SeriesIdResolver;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v1.TypeFactory;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.SystemMetadata;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseReprocessSubprocessor implements IDocumentSubprocessor {

    @Autowired
    private D1IndexerSolrClient d1IndexerSolrClient;

    @Autowired
    private String solrQueryUri;

    @Autowired
    private IndexTaskGenerator indexTaskGenerator;

    private PerformanceLogger perfLog = PerformanceLogger.getInstance();
    
    private List<String> matchDocuments = null;

    private List<String> relationFields;

    public static Log log = LogFactory.getLog(BaseReprocessSubprocessor.class);

    private static boolean bypass = true;
    
    public BaseReprocessSubprocessor() {
    }


    /**
     * If the item has a seriesId, and is not the only one, create IndexTasks for
     * the others in the series, so they ick up the relationship fields, too.
     * @throws EncoderException 
     * @throws IOException 
     * @throws XPathExpressionException 
     * 
     */
    @Override
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            InputStream is) throws XPathExpressionException, IOException, EncoderException { //throws Exception {

        
        ////////     do nothing if can't find the systemMetadata or it has no seriesId
     
        if (bypass)
            return docs;
        
        long getSysMetaStart = System.currentTimeMillis();
        
        SystemMetadata sysMeta = HazelcastClientFactory.getSystemMetadataMap().get(TypeFactory.buildIdentifier(identifier));
        perfLog.log("BaseReprocessSubprocessor.processDocument() HazelcastClientFactory.getSystemMetadataMap().get(id) for id "+identifier, 
                System.currentTimeMillis() - getSysMetaStart);
        
        if (sysMeta == null || sysMeta.getSeriesId() == null) {
            return docs;
        }
        
        log.debug("seriesId===" + sysMeta.getSeriesId().getValue());

        
        
        ////////     do nothing if there are no other series members to act on
        
        long getIdsInSeriesStart = System.currentTimeMillis();
        
        List<SolrDoc> previousDocs = d1IndexerSolrClient.getDocumentsByField(solrQueryUri,
                Collections.singletonList(sysMeta.getSeriesId().getValue()),
                SolrElementField.FIELD_SERIES_ID, true);
        
        perfLog.log(
                "BaseReprocessSubprocessor.processDocument() d1IndexerSolrClient.getDocumentsByField(idsInSeries) for id "+identifier, 
                System.currentTimeMillis() - getIdsInSeriesStart);

        if (previousDocs == null || previousDocs.isEmpty()) {
            return docs;
        }

        
        
        ////// for each identifier in each relationField in each of the retrieved index documents, 
        ////// reprocess the head pid of the series, if the relation was defined using a SID
        ////// (does nothing if the identifier can't be found 
        
        Set<String> pidsToReprocess = new HashSet<>();
        for (SolrDoc indexedDoc : previousDocs) {
            log.debug("indexedDoc===" + indexedDoc);

            for (String fieldName : relationFields) {
                log.debug("fieldName===" + fieldName);

                //////  are there relations that need to be reindexed?
                List<String> relationFieldValues = indexedDoc.getAllFieldValues(fieldName);
                if (relationFieldValues == null) {
                    continue;
                }

                for (String relationFieldValue : relationFieldValues) {
                    Identifier relatedId = TypeFactory.buildIdentifier(relationFieldValue);

                    try {
                        // if it's a sid, resolve to a pid
                        SystemMetadata relatedSysMeta = HazelcastClientFactory.getSystemMetadataMap().get(relatedId);
                        if (relatedSysMeta == null) {

                            // calls cn.getSystemMetadata() which resolves SIDs to PIDs
                            Identifier pid = SeriesIdResolver.getPid(relatedId);
                            relatedId = pid;
                        }

                        // don't reprocess if the item was already reprocessed in this method call.
                        if (! pidsToReprocess.contains(relatedId.getValue())) {
                            pidsToReprocess.add(relatedId.getValue());

                            log.debug("Processing relatedPid===" + relatedId.getValue());
                            
                            // queue a create a new IndexTask of this related document
                            relatedSysMeta = HazelcastClientFactory.getSystemMetadataMap().get(relatedId);
                            String objectPath = HazelcastClientFactory.getObjectPathMap().get(relatedId);

                            log.debug("Processing relatedSysMeta===" + relatedSysMeta);
                            log.debug("Processing objectPath===" + objectPath);

                            indexTaskGenerator.processSystemMetaDataUpdate(relatedSysMeta, objectPath);
                        }
                    }
                    catch (BaseException be) {
                        log.error("Could not locate PID for given identifier: " + relatedId.getValue(), be);
                        // nothing we can do but continue
                        continue;
                    }
                }
            }
        }
        perfLog.log("BaseReprocessSubprocessor.processDocument() reprocessing all docs earlier in sid chain for id "+identifier, System.currentTimeMillis() - getIdsInSeriesStart);

        return docs;
        
    }

    
    /**
     * returns true if formatId is in the matchDocuments list
     * OR if the matchDocuments list is null and the formatId is not that of type 'RESOURCE'
     */
    @Override
    public boolean canProcess(String formatId) {
        // if we are given match formats, use them
        if (matchDocuments != null) {
            return matchDocuments.contains(formatId);
        }

        // otherwise just make sure it's not a RESOURCE type

        try {
            ObjectFormat objectFormat = ObjectFormatCache.getInstance().getFormat(TypeFactory.buildFormatIdentifier(formatId));
            if (objectFormat != null) {
                return !objectFormat.getFormatType().equalsIgnoreCase("RESOURCE");
            } 
            else  {
                // no real harm processing again
                return true;
            }
        } catch (BaseException e) {
            log.warn("No format found in ObjectFormatCache for format '" + formatId + "'",e);
            return true;
        }
        
    }

    @Override
    public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument) throws IOException,
            EncoderException, XPathExpressionException {
        // just return the given document
        return indexDocument;
    }

    public List<String> getRelationFields() {
        return relationFields;
    }

    public void setRelationFields(List<String> relationFields) {
        this.relationFields = relationFields;
    }

}
