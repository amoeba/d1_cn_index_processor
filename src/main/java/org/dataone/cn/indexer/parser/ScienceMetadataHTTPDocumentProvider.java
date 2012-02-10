package org.dataone.cn.indexer.parser;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/28/11
 * Time: 12:32 PM
 */

/**This class has not yet been completed!  Should be updated as soon as a services is available for retrieving Science Metadata via http.
 *
 */
public class ScienceMetadataHTTPDocumentProvider implements IDocumentProvider{



    public Document GetDocument(String identifier) throws ParserConfigurationException, IOException, SAXException {
        return null;
    }
}
