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

package org.dataone.cn.indexer.parser;

import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;

/**
 * 
 * Subprocessors test xml expression with return type of boolean.  
 * canProcess function returns true subprocessor is run 
 * and can add, remove, or manipulate existing fields in document to be indexed 
 * and documents associated to the indexed
 * document.
 *
 * User: Porter
 * Date: 9/22/11
 * Time: 1:33 PM
 */
public interface IDocumentSubprocessor {

    /**Determines if subprocessor should be run on document
     * @param
     * @return returns true if subprocessor should be run on document
     * @throws XPathExpressionException
     */
    public boolean canProcess(Document doc) throws XPathExpressionException;

    public void initExpression(XPath xpath);

    /**
     * Method allows for manipulation of indexed fields that should be added to solr index
     *
     * @param identifier id of original document in Map docs
     * @param docs Documents indexed by identifiers
     * @param doc original XML document
     * @return Updated solr index documents
     */
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            Document doc) throws Exception;
}
