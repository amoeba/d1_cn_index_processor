package org.dataone.cn.indexer.parser;

import java.util.List;

import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

public class FullTextSolrField extends SolrField {

    public FullTextSolrField(String name, String xpath) {
        super(name, xpath);
    }

    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        List<SolrElementField> fields = super.getFields(doc, identifier);
        SolrElementField field = fields.get(0);
        if (field != null) {
            field.setValue(field.getValue().concat(" " + identifier));
        }
        return fields;
    }
}
