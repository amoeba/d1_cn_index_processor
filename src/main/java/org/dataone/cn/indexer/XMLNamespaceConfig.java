package org.dataone.cn.indexer;


import javax.xml.namespace.NamespaceContext;
import java.util.*;

/**
  * User: Porter
 * Date: 7/25/11
 * Time: 4:32 PM
 */
public class XMLNamespaceConfig implements NamespaceContext {

    /** Used in XPathDocumentParser holds a list of prefixes and namespaces for use in XPath Queries
     *
     * @see XPathDocumentParser
     */

    private List<XMLNamespace> namespaceList = null;

    /**
     * @param namespaceList
     */
    public XMLNamespaceConfig(List<XMLNamespace> namespaceList){
        this.namespaceList = namespaceList;
    }

    public String getNamespaceURI(String prefix) {
        for (XMLNamespace xmlNamespace : namespaceList) {
            if(xmlNamespace.getPrefix().equals(prefix)){
                return xmlNamespace.getNamespace();
            }
        }
        return null;
    }

    public String getPrefix(String namespaceURI) {
        for (XMLNamespace xmlNamespace : namespaceList) {
            if(xmlNamespace.getNamespace().equals(namespaceURI)){
                return xmlNamespace.getPrefix();
            }
        }
        return null;
    }

    private Set prefixes = null;
    public Iterator getPrefixes(String namespaceURI) {

        if(prefixes == null){
            prefixes = new HashSet();
            for (XMLNamespace xmlNamespace : namespaceList) {
                prefixes.add(xmlNamespace.getPrefix());
            }
        }
        return prefixes.iterator();
    }


}
