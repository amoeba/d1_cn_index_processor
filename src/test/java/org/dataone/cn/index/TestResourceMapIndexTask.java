package org.dataone.cn.index;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.ResourceMapIndexTask;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestResourceMapIndexTask {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        
        IndexTask t = new IndexTask();
        t.setDateSysMetaModified(System.currentTimeMillis());
        t.setDeleted(false);
        t.setFormatId("text/xml");
        t.setId(1234L);
        t.setPid("foooooo");
        t.setStatus("theStatus");
        t.setTryCount(45);
        t.setVersion(3);
        
        ResourceMapIndexTask rmit = new ResourceMapIndexTask();
        rmit.copy(t);
        List<String> referencedIds = new ArrayList<>();
        referencedIds.add("urn:uuid:3c25e86d-2874-4323-b391-8e62bed2f368");
        referencedIds.add("urn:uuid:78ca8324-f73f-433e-bea4-fe47bc3cb9cd");
        referencedIds.add("urn:uuid:5e3b3f6c-b27d-47b2-a900-8f558bd54d3b");
        referencedIds.add("urn:uuid:b673e37c-557c-4064-86e2-7f428cf21b0e");
        
        
        rmit.setReferencedIds(referencedIds);
        

        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(rmit);
        out.flush();
        out.close();

        byte[] bytes = baos.toByteArray();
        
        ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));
        ResourceMapIndexTask tt = (ResourceMapIndexTask) in.readObject();
        in.close();
        
        assertTrue(tt.getPid().equals("foooooo"));
        assertTrue(tt.getReferencedIds().size()==4);
        assertTrue(tt.getReferencedIds().get(2).equals("urn:uuid:5e3b3f6c-b27d-47b2-a900-8f558bd54d3b"));
    
    
    }

}
