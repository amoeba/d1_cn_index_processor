package org.dataone.cn.indexer.parser;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/6/11
 * Time: 11:09 AM
 */

/**IDocumentProvider use to retrieve resourcemap data from different repositories.
 *
 */
public interface IDocumentProvider {
    public Document GetDocument(String identifier) throws ParserConfigurationException, IOException, SAXException;
}
