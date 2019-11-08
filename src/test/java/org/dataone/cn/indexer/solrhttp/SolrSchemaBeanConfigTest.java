package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;


@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "test-solr-schema.xml" })
public class SolrSchemaBeanConfigTest {

    
    @Autowired
    SolrSchema solrSchema;
    
    @Test
    public void testSegmentsLoaded() {
        assertTrue("loaded schema should have segments", solrSchema.listSegments().size() > 0);
        assertEquals("has 8 segments",solrSchema.listSegments().size(),8);
        assertTrue("has 'internal' ", solrSchema.listSegments().contains("internal"));    
        assertTrue("has 'systema' ", solrSchema.listSegments().contains("sysmeta"));    
        assertTrue("has 'scimeta' ", solrSchema.listSegments().contains("scimeta"));    
        assertTrue("has 'ore' ", solrSchema.listSegments().contains("ore"));    
        assertTrue("has 'prov' ", solrSchema.listSegments().contains("prov"));    
        assertTrue("has 'sem' ", solrSchema.listSegments().contains("sem"));    
        assertTrue("has 'collections' ", solrSchema.listSegments().contains("collections"));    
        assertTrue("has 'mn_service' ", solrSchema.listSegments().contains("mn_service"));    
        }
        
    @Test
    public void testMultiValued() {
//        SolrSchema freshOne = new SolrSchema();
//        freshOne.setSolrSchemaPath("");
        
    
        assertFalse("size should be singleValued", solrSchema.isFieldMultiValued("size"));
        assertTrue("'isDocumentedBy' should be multivalued", solrSchema.isFieldMultiValued("isDocumentedBy"));
    }
    
    @Test
    public void testListSegments() {
        Set<String> segments = solrSchema.listSegments();
        for (String seg : segments) {
            System.out.println(seg);
            List<String> segFields = solrSchema.getAllSegmentFields(seg);
            for (String f : segFields) {
                System.out.println("   * " + f);
            }
        }
    }
    
}
