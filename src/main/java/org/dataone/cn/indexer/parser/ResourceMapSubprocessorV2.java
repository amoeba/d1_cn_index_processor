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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.AbstractStubMergingSubprocessor;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.types.v1.Identifier;
import org.dspace.foresite.OREParserException;


/**
 * Resource Map Document processor.  Operates on ORE/RDF objects.  Maps 
 * 'documents', 'documentedBy', and 'aggregates' relationships.
 * 
 * Uses org.dataone.cn.indexer.resourcemap.ResourceMap to update individual
 * SolrDoc objects with values for 'documents', 'documentedBy', and 'resourceMap'
 * (aggregates) fields.
 * 
 * Updates entries for related documents in index. For document relational
 * information refer to
 * http://purl.dataone.org/architecture/design/SearchMetadata.html#id4
 * 
 * Date: 9/26/11
 * Time: 3:51 PM
 */
public class ResourceMapSubprocessorV2 extends AbstractStubMergingSubprocessor implements IDocumentSubprocessorV2 {

    private static Logger logger = Logger.getLogger(ResourceMapSubprocessorV2.class.getName());

    private PerformanceLogger perfLog = PerformanceLogger.getInstance();
    
    private List<String> matchDocuments = null;
    private List<String> fieldsToMerge = new ArrayList<String>();
    
    /**
     * Given the starting SolrDoc for the resourcemap (from upstream processors), and parsed XML,
     * get all the members,  
     */
     @Override
     protected Map<String,SolrDoc> parseDocument(String identifier, InputStream resourceMapStream)
            throws Exception {

        long buildResMapStart = System.currentTimeMillis();
          
        // this new way of building the solr records doesn't yet deal with seriesId in the relationship fields, 
        // or calling clearObsoletesChain to do whatever it is that it's supposed to do...
        
        Map<Identifier, Map<Identifier, List<Identifier>>> tmpResourceMap = null;

        try {
            tmpResourceMap = org.dataone.ore.ResourceMapFactory.getInstance().parseResourceMap(resourceMapStream);

        } catch (Throwable e) {
            logger.error("Unable to parse ORE document:", e);
            throw new OREParserException(e);
        }
        perfLog.log("ResourceMapFactory.buildResourceMap() create ResourceMap from InputStream", System.currentTimeMillis() - buildResMapStart);
        
        Entry<Identifier, Map<Identifier, List<Identifier>>> resMapHierarchy = tmpResourceMap.entrySet().iterator().next();
       
        String resourceMapId = resMapHierarchy.getKey().getValue();
        
        Map<Identifier,List<Identifier>> metadataMap = resMapHierarchy.getValue();
        Map<String,SolrDoc> memberDocs = new HashMap<>();
        for (Identifier mId : metadataMap.keySet()) {
            String mdId = mId.getValue();
            // create the metadata SolrDoc
            if (!memberDocs.containsKey(mdId)) {
                memberDocs.put(mdId,new SolrDoc());
                memberDocs.get(mdId).addField(new SolrElementField("id",mdId));
                memberDocs.get(mdId).addField(new SolrElementField("resourceMap",resourceMapId));
            }
            
            for (Identifier dId : metadataMap.get(mId)) {
                String dataId = dId.getValue();
                // create the data SolrDocs
                if (!memberDocs.containsKey(dataId)) {
                    memberDocs.put(dataId, new SolrDoc());
                    memberDocs.get(dataId).addField(new SolrElementField("id",dataId));
                    memberDocs.get(dataId).addField(new SolrElementField("resourceMap",resourceMapId));
                }
                // add the relationships
                memberDocs.get(dataId).addField(new SolrElementField("isDocumentedBy",mdId));
                memberDocs.get(mdId).addField(new SolrElementField("documents",dataId));

            }
        }
        return memberDocs;
    }

  
    public List<String> getMatchDocuments() {
        return matchDocuments;
    }

    public void setMatchDocuments(List<String> matchDocuments) {
        this.matchDocuments = matchDocuments;
    }

    public boolean canProcess(String formatId) {
        return matchDocuments.contains(formatId);
    }
    
    public List<String> getFieldsToMerge() {
        return fieldsToMerge;
    }

    public void setFieldsToMerge(List<String> fieldsToMerge) {
        this.fieldsToMerge = fieldsToMerge;
    }
}
