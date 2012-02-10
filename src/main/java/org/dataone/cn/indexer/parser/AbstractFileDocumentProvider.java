package org.dataone.cn.indexer.parser;

import org.dataone.cn.indexer.XPathDocumentParser;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/28/11
 * Time: 12:33 PM
 */

/**Document Provider designed to load documents based off of file name and predefined directory.
 *
 */
public class AbstractFileDocumentProvider implements IDocumentProvider{

    private String baseDirectory = null;
    private DocumentBuilder documentBuilder;


    public Document GetDocument(String identifier) throws ParserConfigurationException, IOException, SAXException {

        File f = new File(baseDirectory, identifier + ".xml");

        return GetDocumentFromFile(f);
    }

    public Document GetDocumentFromFile(File f) throws ParserConfigurationException, IOException, SAXException {
        Document xmlFile = parseDocument(f);
        return xmlFile;
    }


    public String getBaseDirectory() {
        return baseDirectory;
    }

    public void setBaseDirectory(String baseDirectory) {
        this.baseDirectory = baseDirectory;
    }

    private Document parseDocument(File f){
        try {
            Document doc = XPathDocumentParser.getDocumentBuilder().parse(f);
            return doc;


        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
