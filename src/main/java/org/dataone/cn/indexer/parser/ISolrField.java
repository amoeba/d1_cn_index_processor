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

import java.util.List;

import javax.xml.xpath.XPath;

import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

/**
 * Created by IntelliJ IDEA. User: Porter Date: 9/22/11 Time: 1:31 PM
 */
public interface ISolrField {
    /**
     * Method for initializing xpath expression from main document builder.
     * 
     * @param xpathObject
     */
    public void initExpression(XPath xpathObject);

    /**
     * Method for extracting data from document via XPath or other means.
     * 
     * @param doc
     * @return Data Elements parsed from xml document
     * @throws Exception
     */
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception;

    public String getName();
}
