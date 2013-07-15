package org.dataone.cn.indexer.resourcemap;

import org.dspace.foresite.OREParserException;
import org.w3c.dom.Document;

/**
 * Provides concrete instances of ResourceMap interface.  Hides use of implementation class
 * from clients.
 * 
 * @author sroseboo
 *
 */
public class ResourceMapFactory {

    private ResourceMapFactory() {
    }

    public static ResourceMap buildResourceMap(String objectFilePath) throws OREParserException {
        return new ForesiteResourceMap(objectFilePath);
    }

    public static ResourceMap buildResourceMap(Document oreDoc) throws OREParserException {
        return new ForesiteResourceMap(oreDoc);
    }
}
