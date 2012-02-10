package org.dataone.cn.indexer.resourcemap;

import org.dataone.cn.indexer.parser.AbstractFileDocumentProvider;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/1/11
 * Time: 12:39 PM
 */


/**Retrieves resource map from file system from predefined directory with the file name PID + ".xml"
 *
 */
public class ResourceMapFileDataSource extends AbstractFileDocumentProvider {

    @Override
    public Document GetDocument(String identifier) throws ParserConfigurationException, IOException, SAXException {
        return super.GetDocument(identifier);
    }
}
