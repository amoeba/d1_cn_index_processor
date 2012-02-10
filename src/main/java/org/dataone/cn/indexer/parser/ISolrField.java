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
}
