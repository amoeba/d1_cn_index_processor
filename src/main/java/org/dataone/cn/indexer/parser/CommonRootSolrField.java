package org.dataone.cn.indexer.parser;

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPath;

import org.apache.commons.lang.StringEscapeUtils;
import org.dataone.cn.indexer.parser.utility.LogicalOrPostProcessor;
import org.dataone.cn.indexer.parser.utility.RootElement;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

public class CommonRootSolrField extends SolrField {

    private RootElement root;

    private LogicalOrPostProcessor orProcessor = new LogicalOrPostProcessor();

    public CommonRootSolrField(String name) {
        this.name = name;
    }

    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        if (root != null) {
            List<String> resultValues = root.getRootValues(doc, isMultivalue());
            for (String value : resultValues) {
                if (getConverter() != null) {
                    value = getConverter().convert(value);
                }
                if (isEscapeXML()) {
                    value = StringEscapeUtils.escapeXml(value);
                }

                if (orProcessor != null) {
                    value = orProcessor.process(value);
                }

                fields.add(new SolrElementField(this.name, value));
                if (!isMultivalue()) {
                    break;
                }
            }
        }
        return fields;
    }

    @Override
    public void initExpression(XPath xpathObject) {
        root.initXPathExpressions(xpathObject);
    }

    public RootElement getRoot() {
        return root;
    }

    public void setRoot(RootElement root) {
        this.root = root;
    }
}
