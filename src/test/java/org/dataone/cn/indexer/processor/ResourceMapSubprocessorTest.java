package org.dataone.cn.indexer.processor;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Quad;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.service.types.v1.Identifier;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;


public class ResourceMapSubprocessorTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.out.println("hello?");
    }

    @Before
    public void setUp() throws Exception {
    }

  
    @Test
    public void testTrue() {
        ;
    }
    
//    @Test
    public void testResourceMapSIDparsing()  throws Exception {
        
        Map<Identifier, Map<Identifier, List<Identifier>>> tmpResourceMap = null;

        InputStream is = this.getClass().getResourceAsStream(
                "packageIndexingTest_P_S_S_T_T_T_20180412.12.12_resMap.1.rdf");

        tmpResourceMap = org.dataone.ore.ResourceMapFactory.getInstance().parseResourceMap(is);
 

        
        Map<Identifier,SolrDoc> memberDocs = new HashMap<>();
        Entry<Identifier, Map<Identifier, List<Identifier>>> resMapHierarchy = tmpResourceMap.entrySet().iterator().next();
        String resourceMapId = resMapHierarchy.getKey().getValue();
        Map<Identifier,List<Identifier>> metadataMap = resMapHierarchy.getValue();
        for (Identifier mdId : metadataMap.keySet()) {
            if (!memberDocs.containsKey(mdId)) {
                memberDocs.put(mdId,new SolrDoc());
                memberDocs.get(mdId).addField(new SolrElementField("id",mdId.getValue()));
                memberDocs.get(mdId).addField(new SolrElementField("resourceMap",resourceMapId));
            }
            for (Identifier dataId : metadataMap.get(mdId)) {
                if (!memberDocs.containsKey(dataId)) {
                    memberDocs.put(dataId, new SolrDoc());
                    memberDocs.get(dataId).addField(new SolrElementField("id",dataId.getValue()));
                    memberDocs.get(dataId).addField(new SolrElementField("resourceMap",resourceMapId));
                }
                memberDocs.get(dataId).addField(new SolrElementField("isDocumentedBy",mdId.getValue()));
                memberDocs.get(mdId).addField(new SolrElementField("documents",dataId.getValue()));

            }
        }
//        List<SolrDoc> allMembers = new ArrayList<>();
//        SolrDoc indexDocument 
//        allMembers.add(indexDocument);
//        allMembers.addAll(memberDocs.values());

    }

    
    @Test
    public void testRDFParsing() throws Exception {
        
        String identifierString = "packageIndexingTest_P_S_S_T_T_T_20180412.12.12_resMap.1";
        
        InputStream resourceStream = this.getClass().getResourceAsStream("packageIndexingTest_P_S_S_T_T_T_20180412.12.12_resMap.1.rdf");
        
        boolean quoteLiterals = false;
        PrefixMapping pm = PrefixMapping.Standard; // TODO: should we use Extended or a custom mapping?
        
        List<SolrInputDocument> solrDocs = new ArrayList<>();
        
        Iterator<Triple> tripleIt = RDFDataMgr.createIteratorTriples(resourceStream, Lang.RDFXML,"");
        while (tripleIt.hasNext()) {
            Triple t = tripleIt.next();
            
            // create a solrDoc to send to update
            SolrInputDocument sd = new SolrInputDocument();
            sd.addField("sourceId",identifierString);
            sd.addField("subjectId",t.getSubject().toString(pm, quoteLiterals));
            sd.addField("predicate",t.getPredicate().toString(pm, quoteLiterals));
            sd.addField("object",t.getObject().toString(pm, quoteLiterals));
//            sd.addField(new SolrElementField("canChange",identifierString));
            solrDocs.add(sd);
            System.out.println(t.toString(PrefixMapping.Standard));
        }
      
//        HTTPService client = new HTTPService(new HttpComponentsClientHttpRequestFactory());
        
        SolrClient client = new ConcurrentUpdateSolrClient
                                            .Builder("http://localhost:8983/solr")
                                            .build();
        
        UpdateResponse resp = client.add("d1-cn-relationships", solrDocs);
        
        System.out.println(resp.toString());
        
        Thread.sleep(50000);
        
      
    }
}
