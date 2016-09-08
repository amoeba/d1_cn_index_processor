package org.dataone.cn.indexer;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

public class XmlDocumentUtilityTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {

    }

    @Test
    public void testUnicodeHandling() throws SAXException {
        
        InputStream is = ClassLoader.getSystemResourceAsStream("org/dataone/cn/indexer/GriidcExample.xml");
        Document doc = XmlDocumentUtility.generateXmlDocument(is);
        
//        add.serialize(outputStream, encoding);
//        outputStream.flush();
//        outputStream.close();

    }
    


}
