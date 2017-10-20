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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.dataone.cn.indexer.solrhttp.SolrDoc;

/**
 * 
 * Subprocessors test xml expression with return type of boolean.  
 * canProcess function returns true subprocessor is run 
 * and can add, remove, or manipulate existing fields in document to be indexed 
 * and documents associated to the indexed
 * document.
 *
 * Date: 9/22/11
 * Time: 1:33 PM
 */
public interface IDocumentSubprocessor {

    /**Determines if sub-processor should be run on document
     * @param formatId for the object being tested
     * @return returns true if sub-processor should be run on document
     */
    public boolean canProcess(String formatId);

    /**
     * Method allows for accumulation of indexed fields that should be added to solr index.
     * Subprocessors have the ability to overview previous values from other processors, so
     * should be careful when modifying the SolrDoc map.
     *
     * @param identifier: 
     *            the id of object that is being parsed 
     * @param docs: 
     *            the map of SolrDocs representing the accumulated parsed values
     * @param is: 
     *            the object to process, as an input stream
     * @return map of SolrDocs, one for each index record that will be updated
     */
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            InputStream is) throws Exception;

    
    
    
    /**
     * Compares the given SolrDoc with what's in the solr index to return the cleaned up
     * version that contain the appropriate field values, originating from either source.
     * (This can be either a diff or superset)
     * @param indexDocument
     * @return
     * @throws IOException
     * @throws EncoderException
     * @throws XPathExpressionException
     */
    public SolrDoc mergeWithIndexedDocument(SolrDoc indexDocument) throws IOException,
            EncoderException, XPathExpressionException;
}