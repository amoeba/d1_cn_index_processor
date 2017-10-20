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

import org.dataone.cn.indexer.solrhttp.SolrDoc;

public interface IDocumentDeleteSubprocessor {

    /**
     * returns a map of documents that need to be processed as updates.  Entries with null values (key with null value)
     * should be processed as a new task (reindexed)
     * @param identifier
     * @param docs
     * @return
     * @throws Exception
     */
    public Map<String, SolrDoc> processDocForDelete(String identifier, Map<String, SolrDoc> docs)
            throws Exception;
}
