package org.dataone.cn.indexer.solrhttp;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

/**
 * Command for adding or updating documents in Solr index
 * 
 * example output:
 * 
 * <pre>
 *  {@code
 *           <add>
 *         <doc>
 *           <field name="employeeId">05991</field>
 *           <field name="office">Bridgewater</field>
 *           <field name="skills">Perl</field>
 *           <field name="skills">Java</field>
 *         </doc>
 *         [<doc> ... </doc>[<doc> ... </doc>]]
 * </add>
 * }
 * </pre>
 * 
 */

public class SolrElementAdd {

    private static final char[] ELEMENT_ADD_OPEN = "<add>".toCharArray();
    private static final char[] ELEMENT_ADD_CLOSE = "</add>".toCharArray();
    private List<SolrDoc> docList = new ArrayList<SolrDoc>();

    public SolrElementAdd() {

    }

    public SolrElementAdd(List<SolrDoc> docs) {
        this.docList = docs;
    }

    /**
     * Writes data to (http) output stream with the specified encoding
     * 
     * @param outputStream
     * @param encoding
     * 
     * @throws IOException
     * 
     */
    public void serialize(OutputStream outputStream, String encoding) throws IOException {

        IOUtils.write("<?xml version=\"1.1\" encoding=\"utf-8\"?>\n", outputStream, encoding);
        IOUtils.write(ELEMENT_ADD_OPEN, outputStream, encoding);

        for (SolrDoc doc : docList) {
            doc.serialize(outputStream, encoding);
        }
        IOUtils.write(ELEMENT_ADD_CLOSE, outputStream, encoding);

    }

    public List<SolrDoc> getDocList() {
        return docList;
    }

    public void setDocList(List<SolrDoc> docList) {
        this.docList = docList;
    }
}
