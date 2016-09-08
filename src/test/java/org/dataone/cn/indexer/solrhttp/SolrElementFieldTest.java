package org.dataone.cn.indexer.solrhttp;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.dataone.cn.indexer.XmlDocumentUtility;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class SolrElementFieldTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSolrDocumentXmlParsing() throws IOException, SAXException {
        
        // this document is the one with the troublesome character
        InputStream is = ClassLoader.getSystemResourceAsStream("org/dataone/cn/indexer/GriidcExample.xml");
        Document doc = XmlDocumentUtility.generateXmlDocument(is);
        // we know it is in the following field
        NodeList nl = doc.getElementsByTagName("gmd:supplementalInformation");
        String text = nl.item(0).getTextContent();
        System.out.println(text);

        // set it as the value that will be serialized then deserialized
        // (xml-encoded then decoded)
        SolrElementField field = new SolrElementField("foo",text);
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        field.serialize(os, "UTF-8");

        ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray());
        try {
            XmlDocumentUtility.generateXmlDocument(bais);
        } catch (SAXException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            fail("Should not throw parsing exception when ingesting the solr document!");
        }

    }

}
