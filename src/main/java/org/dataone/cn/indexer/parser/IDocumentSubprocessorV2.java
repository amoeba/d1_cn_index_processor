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

import java.io.InputStream;

/**
 * 
 * Subprocessors test xml expression with return type of boolean.  
 * canProcess function returns true subprocessor is run 
 * <p/>
 * Concrete Subprocessors are expected to collect parsed values of the object into 
 * the updateAssembler parameter to processDocument, and MUST collect any 
 * SolrDocuments returned from solr queries.  According to the UpdateAssumbler
 * semantics, any retrieved values from solr are collected separately from
 * the parsed values.
 * 
 * Date: 7/10/19
 */
public interface IDocumentSubprocessorV2 {

    /**Determines if sub-processor should be run on document
     * @param formatId for the object being tested
     * @return returns true if sub-processor should be run on document
     */
    public boolean canProcess(String formatId);

    /**
     * Method allows for accumulation of indexed fields that should be added to solr index.
     * Subprocessors are expected to simply add parsed values as a new update, any Solr-retrieved
     * documents into the updateAssembler, and leave any merge behavior to the UpdateAssembler
     * at a later stage.
     *
     * @param identifier: 
     *            the id of object that is being parsed 
     * @param docs: 
     *            the starting updateAssembler instance containing the collected updates for the task
     * @param is: 
     *            the object to process, as an input stream 
     */
    public void processDocument(String identifier, UpdateAssembler docs, InputStream is) throws Exception;

}

    
 