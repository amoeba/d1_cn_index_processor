/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For 
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright ${year}
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 * 
 * $Id$
 */

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

class SolrElementAdd {

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

        IOUtils.write("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n", outputStream, encoding);
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
