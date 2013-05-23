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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;

import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.w3c.dom.Document;

/**
 * Created by IntelliJ IDEA.
 * User: Porter
 * Date: 9/22/11
 * Time: 3:24 PM
 */

/**
 * Base functionality for Sub processor.
 * 
 */

public class AbstractDocumentSubprocessor implements IDocumentSubprocessor {

    /**
     * If xpath returns true execute the processDocument Method
     */
    private String matchDocument = null;
    private XPathExpression matchDocumentExpression = null;
    private List<ISolrField> fieldList = new ArrayList<ISolrField>();

    public String getMatchDocument() {
        return matchDocument;
    }

    public void setMatchDocument(String matchDocument) {
        this.matchDocument = matchDocument;
    }

    /**
     * Default functionality is to process fields like XPathDocumentProcessor
     * and add fields to Solr Document This method maybe overridden to add
     * functionality such as retrieving and updating existing documents in the
     * index.
     * 
     * @param identifier
     *            identifier of System Metadata Document
     * @param docs
     *            Map of Solr Index documents use @identifier to retrieve the
     *            original System Metadata Document
     * @param doc
     *            System Metadata Document
     * @return map of Documents including updated Solr index document
     */
    public Map<String, SolrDoc> processDocument(String identifier, Map<String, SolrDoc> docs,
            Document doc) throws Exception {

        SolrDoc metaDocument = docs.get(identifier);

        for (ISolrField solrField : fieldList) {
            try {
                metaDocument.getFieldList().addAll(solrField.getFields(doc, identifier));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return docs;
    }

    /**
     * Returns true if subprocessor should be run against document.
     * 
     * @param doc
     * @return
     * @throws XPathExpressionException
     */
    public boolean canProcess(Document doc) throws XPathExpressionException {

        Boolean matches = (Boolean) matchDocumentExpression.evaluate(doc, XPathConstants.BOOLEAN);
        return matches == null ? false : matches.booleanValue();
    }

    public void initExpression(XPath xpathObject) {
        try {
            matchDocumentExpression = xpathObject.compile(getMatchDocument());
            for (ISolrField solrField : fieldList) {
                solrField.initExpression(xpathObject);
            }
        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    public List<ISolrField> getFieldList() {
        return fieldList;
    }

    public void setFieldList(List<ISolrField> fieldList) {
        this.fieldList = fieldList;
    }
}
