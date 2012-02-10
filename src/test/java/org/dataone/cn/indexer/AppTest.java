package org.dataone.cn.indexer;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementAdd;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.xml.sax.SAXException;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
    /**
     * Create the test case
     * 
     * @param testName
     *            name of the test case
     */
    public AppTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(AppTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() throws IOException, SAXException, ParserConfigurationException {
    }

    public void testAddElement() throws Exception {

        List<SolrElementField> fieldList = new ArrayList<SolrElementField>();
        fieldList.add(new SolrElementField("fieldName1", "value1"));
        fieldList.add(new SolrElementField("fieldName2", "value2"));
        fieldList.add(new SolrElementField("fieldName3", "value3"));
        fieldList.add(new SolrElementField("fieldName4", "value4"));
        fieldList.add(new SolrElementField("fieldName5", "value5"));
        fieldList.add(new SolrElementField("fieldName6", "value6"));
        SolrDoc doc = new SolrDoc(fieldList);
        SolrElementAdd add = new SolrElementAdd();
        add.getDocList().add(doc);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BufferedOutputStream bos = new BufferedOutputStream(baos);
        add.serialize(bos, "UTF-8");
        bos.flush();
        baos.flush();
        System.out.println("output: " + new String(baos.toByteArray(), "UTF-8"));
    }
}
