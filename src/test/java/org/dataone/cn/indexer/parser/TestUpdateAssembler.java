package org.dataone.cn.indexer.parser;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.SolrIndexServiceV2;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.cn.indexer.solrhttp.SolrSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestUpdateAssembler {

    static ApplicationContext context;
    static SolrSchema schema;
    
    @BeforeClass
    public static void setUp() throws Exception {
        
        context = new ClassPathXmlApplicationContext("/org/dataone/cn/indexer/solrhttp/test-solr-schema.xml");
        schema = (SolrSchema) context.getBean("solrSchema");       
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testAssembleUpdate_ExistingOnlyReturnsNullList() throws IOException {
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        assertTrue("_version_ is a valid field", schema.getValidFields().contains("_version_"));
        
        SolrDoc existing = new SolrDoc();
        existing.updateOrAddField("id", "MD");
        existing.updateOrAddField("resourceMap", "ORE");
        existing.updateOrAddField("documents", "DATA");
        existing.updateOrAddField("_version_", "1234567890");
        ua.addToUpdate("MD",existing,null);
        
        List<SolrDoc> updates = ua.assembleUpdate(0);
        assertEquals(0,updates.size());   
    }
    
    
    @Test
    public void testAssembleUpdate_NoExistingReturnsConsolidateNewMaterial_NoDupes() throws IOException {
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        assertTrue("_version_ is a valid field", schema.getValidFields().contains("_version_"));
        
        SolrDoc newStuff = new SolrDoc();
        newStuff.updateOrAddField("id", "MD");
        newStuff.updateOrAddField("resourceMap", "ORE");
        newStuff.updateOrAddField("documents", "DATA");
        ua.addToUpdate("foo",null,newStuff);
        
        SolrDoc newerStuff = new SolrDoc();
        newerStuff.updateOrAddField("id", "MD");
        newerStuff.updateOrAddField("resourceMap", "ORE");
        newerStuff.updateOrAddField("documents", "DATA");
        ua.addToUpdate("foo",null,newerStuff);
       
        List<SolrDoc> updates = ua.assembleUpdate(0);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        updates.get(0).serialize(baos, "UTF-8");
        System.out.println(baos.toString());
        
        
        assertEquals("ORE",updates.get(0).getFirstFieldValue("resourceMap"));
        assertEquals("DATA",updates.get(0).getFirstFieldValue("documents"));
        assertEquals("MD",updates.get(0).getFirstFieldValue("id"));
        assertEquals(1,updates.size());
        assertEquals(4,updates.get(0).getFieldList().size()); 
        assertEquals(1,updates.get(0).getFields("resourceMap").size());
        assertEquals(1,updates.get(0).getFields("documents").size());
        assertEquals(1,updates.get(0).getFields("id").size());
    }
 
    
    @Test
    public void testAssembleUpdate_NoExistingReturnsConsolidateNewMaterial_1() throws IOException {
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        assertTrue("_version_ is a valid field", schema.getValidFields().contains("_version_"));
        
        SolrDoc newStuff = new SolrDoc();
        newStuff.updateOrAddField("id", "MD");
        newStuff.updateOrAddField("resourceMap", "ORE");
        newStuff.updateOrAddField("documents", "DATA");
//        newStuff.updateOrAddField("_version_","123456");
        ua.addToUpdate("foo",null,newStuff);
       
        List<SolrDoc> updates = ua.assembleUpdate(0);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        updates.get(0).serialize(baos, "UTF-8");
        System.out.println(baos.toString());
        
        
        assertEquals("ORE",updates.get(0).getFirstFieldValue("resourceMap"));
        assertEquals("DATA",updates.get(0).getFirstFieldValue("documents"));
        assertEquals("MD",updates.get(0).getFirstFieldValue("id"));
        // verison -1 is assigned for the update
        assertEquals("-1", updates.get(0).getFirstFieldValue("_version_"));
        assertEquals(1,updates.size());
        assertEquals(4,updates.get(0).getFieldList().size());
        // modifiers ?
        System.out.println("id modifier: " + updates.get(0).getField("id").getModifier());
        System.out.println("resourceMap Modifier: " + updates.get(0).getField("resourceMap").getModifier());
        System.out.println("documents Modifier: " + updates.get(0).getField("documents").getModifier());
        System.out.println("_version_ Modifier: " + updates.get(0).getField("_version_").getModifier());
    }
    
    
    @Test
    public void testAssembleUpdate_ReturnsEmptyList_When_NewMaterialEqualsExisting() throws IOException {
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        assertTrue("_version_ is a valid field", schema.getValidFields().contains("_version_"));
        
        SolrDoc newStuff = new SolrDoc();
        newStuff.updateOrAddField("id", "MD");
        newStuff.updateOrAddField("resourceMap", "ORE");
        newStuff.updateOrAddField("documents", "DATA");
        ua.addToUpdate("MD",null,newStuff);
       
        List<SolrDoc> updates = ua.assembleUpdate(0);
        assertEquals("ORE",updates.get(0).getFirstFieldValue("resourceMap"));
        assertEquals("DATA",updates.get(0).getFirstFieldValue("documents"));
        assertEquals("MD",updates.get(0).getFirstFieldValue("id"));
        assertEquals(1,updates.size());
        assertEquals(4,updates.get(0).getFieldList().size());
        
        SolrDoc existing = new SolrDoc();
        existing.updateOrAddField("id", "MD");
        existing.updateOrAddField("resourceMap", "ORE");
        existing.updateOrAddField("documents", "DATA");
        existing.updateOrAddField("_version_", "1234567890");
        ua.addToUpdate("MD",existing,null);
        
        updates = ua.assembleUpdate(0);
        assertEquals(0,updates.size());
    }
    

    @Test
    public void testAssembleUpdate_NoDuplicateMultiValuedFieldValues() throws IOException {
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        assertTrue("_version_ is a valid field", schema.getValidFields().contains("_version_"));
        
        SolrDoc newStuff = new SolrDoc();
        newStuff.updateOrAddField("id", "MD");
        newStuff.updateOrAddField("identifier", "PID");
        newStuff.updateOrAddField("resourceMap", "ORE");
        newStuff.updateOrAddField("documents", "DATA");
        ua.addToUpdate("MD",null,newStuff);
       
        List<SolrDoc> updates = ua.assembleUpdate(0);
        assertEquals("ORE",updates.get(0).getFirstFieldValue("resourceMap"));
        assertEquals("DATA",updates.get(0).getFirstFieldValue("documents"));
        assertEquals("MD",updates.get(0).getFirstFieldValue("id"));
        assertEquals(1,updates.size());
        assertEquals(5,updates.get(0).getFieldList().size());
        
        SolrDoc existing = new SolrDoc();
        existing.updateOrAddField("id", "MD");
        existing.updateOrAddField("resourceMap", "ORE");
        existing.updateOrAddField("documents", "DATA");
        existing.updateOrAddField("_version_", "1234567890");
        ua.addToUpdate("MD",existing,null);
        
        updates = ua.assembleUpdate(0);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        updates.get(0).serialize(baos, "UTF-8");
        System.out.println(baos.toString());
        
        assertEquals("PID", updates.get(0).getFirstFieldValue("identifier"));
        assertEquals("MD", updates.get(0).getFirstFieldValue("id"));
        assertEquals("1234567890", updates.get(0).getFirstFieldValue("_version_"));
        assertEquals(1, updates.size());
        
        
        assertEquals(0,updates.get(0).getAllFieldValues("resourceMap").size());
        assertEquals(0,updates.get(0).getAllFieldValues("documents").size());
        assertEquals(1,updates.get(0).getAllFieldValues("id").size());
        assertEquals(1,updates.get(0).getAllFieldValues("identifier").size());
        assertEquals(1,updates.get(0).getAllFieldValues("_version_").size());
        assertEquals(3, updates.get(0).getFieldList().size());
    
    }
    
    @Test
    public void testPartialUpdate_ExistingStub() {
        
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        //create the stub
        SolrDoc existing = new SolrDoc();
        existing.updateOrAddField("id", "MD");
        existing.updateOrAddField("resourceMap", "ORE");
        existing.updateOrAddField("documents", "DATA");
        existing.updateOrAddField("_version_", "1234567890");

        
        SolrDoc update = new SolrDoc();
        update.updateOrAddField("id", "MD");
        update.updateOrAddField("formatId", "emlversion2");     // sysmeta field
        update.updateOrAddField("title", "bestPublicationYet"); // metadata field

        ua.addToUpdate("MD",existing,update);
        List<SolrDoc> thePartialUpdate = ua.assembleUpdate(0);
        
        for (SolrElementField sef : thePartialUpdate.get(0).getFieldList()) {
            System.out.println("Name: " + sef.getName());
            System.out.println("Modifier: " + sef.getModifier());
            System.out.println("Value: " + sef.getValue());
            System.out.println("");
        }
        assertEquals("should get version of existing" , "1234567890", 
                thePartialUpdate.get(0).getFirstFieldValue("_version_"));
        
    }
    
    @Test
    public void testPartialUpdate_NoExisting_ShouldDeclareVersionNegativeOne() {
        
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        //create the stub
//        SolrDoc existing = new SolrDoc();
//        existing.updateOrAddField("id", "MD");
//        existing.updateOrAddField("resourceMap", "ORE");
//        existing.updateOrAddField("documents", "DATA");
//        existing.updateOrAddField("_version_", "1234567890");

        
        SolrDoc update = new SolrDoc();
        update.updateOrAddField("id", "MD");
        update.updateOrAddField("formatId", "emlversion2");
        update.updateOrAddField("title", "bestPublicationYet");

        ua.addToUpdate("MD",null,update);
        List<SolrDoc> thePartialUpdate = ua.assembleUpdate(0);
        
        for (SolrElementField sef : thePartialUpdate.get(0).getFieldList()) {
            System.out.println("Name: " + sef.getName());
            System.out.println("Modifier: " + sef.getModifier());
            System.out.println("Value: " + sef.getValue());
            System.out.println("");
        }
        assertEquals("should get version -1" , "-1", thePartialUpdate.get(0).getFirstFieldValue("_version_"));
    }
    
    @Test
    public void testPartialUpdate_MultidocUpdate() {
        
        UpdateAssembler ua = new UpdateAssembler(schema);
        
        //create the stub
        SolrDoc md = new SolrDoc();
        md.updateOrAddField("id", "MD");
        md.updateOrAddField("resourceMap", "ORE");
        md.updateOrAddField("documents", "DATA");

        
        SolrDoc data = new SolrDoc();
        data.updateOrAddField("id", "DATA");
        data.updateOrAddField("resourceMap", "ORE");
        data.updateOrAddField("isDocumentedBy", "MD");

        SolrDoc ore = new SolrDoc();
        ore.updateOrAddField("id", "ORE");
        ore.updateOrAddField("formatId", "ore-resourcemap-terms");
        ore.updateOrAddField("rightsHolder", "me");
        
        ua.addToUpdate("MD",null,md);
        ua.addToUpdate("DATA",null,data);
        ua.addToUpdate("ORE",null,ore);

        List<SolrDoc> thePartialUpdate = ua.assembleUpdate(0);       
        assertEquals("For number of docs in the update ", 3,thePartialUpdate.size());
        
        for (SolrDoc up : thePartialUpdate) {
            System.out.println(up.getFirstFieldValue("id"));
            assertEquals("version should be -1 for id " + up.getFirstFieldValue("id"),"-1",
                    up.getFirstFieldValue("_version_"));
        }
        
        SolrDoc foundExisting = new SolrDoc();
        foundExisting.updateOrAddField("id","MD");
        foundExisting.updateOrAddField("formatId","emlversion2");
        foundExisting.updateOrAddField("_version_","12345123451234");
        foundExisting.updateOrAddField("title","a good paper");
        ua.addToUpdate("MD", foundExisting,  null);
        
        thePartialUpdate = ua.assembleUpdate(1);       
        assertEquals("For number of docs in the update ", 2,thePartialUpdate.size());
        
        assertEquals("12345123451234",thePartialUpdate.get(0).getFirstFieldValue("_version_"));
        assertEquals("MD",thePartialUpdate.get(0).getFirstFieldValue("id"));
        assertEquals(null,thePartialUpdate.get(0).getFirstFieldValue("formatId")); // because already in the index
        assertEquals(null,thePartialUpdate.get(0).getFirstFieldValue("title"));    // becuase already in the index
        assertEquals("ORE",thePartialUpdate.get(0).getFirstFieldValue("resourceMap"));
        assertEquals("DATA",thePartialUpdate.get(0).getFirstFieldValue("documents"));
        assertEquals(SolrElementField.Modifier.ADD, thePartialUpdate.get(0).getField("resourceMap").getModifier());
        assertEquals(SolrElementField.Modifier.ADD, thePartialUpdate.get(0).getField("documents").getModifier());
        
        assertEquals("-1",thePartialUpdate.get(1).getFirstFieldValue("_version_"));
        
        
//        for (SolrElementField sef : thePartialUpdate.get(0).getFieldList()) {
//            System.out.println("Name: " + sef.getName());
//            System.out.println("Modifier: " + sef.getModifier());
//            System.out.println("Value: " + sef.getValue());
//            System.out.println("");
//        }
//        assertEquals("should get version 1234567890" , "1234567890", thePartialUpdate.get(0).getFirstFieldValue("_version_"));
    }
}
