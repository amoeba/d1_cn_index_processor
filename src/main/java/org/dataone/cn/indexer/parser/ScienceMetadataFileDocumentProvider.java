package org.dataone.cn.indexer.parser;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/28/11
 * Time: 12:33 PM
 */

/**Document provider for science metadata.  Current implmentation is designed to look for file in predefined direcotroy
 * by id+".xml";
 *
 */
public class ScienceMetadataFileDocumentProvider extends AbstractFileDocumentProvider{

    @Override
    public Document GetDocument(String identifier) throws ParserConfigurationException, IOException, SAXException {
        return super.GetDocument(identifier);
    }
}
