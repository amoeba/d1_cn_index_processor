package org.dataone.cn.indexer;

/**
  * User: Porter
 * Date: 7/25/11
 * Time: 1:28 PM
 */

/**Helper class for prefix to namespace mappings used in xpath
 *
 */
public class XMLNamespace {

    /**Used to define prefix and namespace for XMLNamespace for xpath queries in XPathDocumentParser
     *
     * @see XMLNamespace
     * @see XPathDocumentParser
     */

    private String prefix = null;
    private String namespace = null;

    public XMLNamespace() {
    }

    public XMLNamespace(String prefix, String namespace) {
        this.prefix = prefix;
        this.namespace = namespace;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
