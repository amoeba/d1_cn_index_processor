package org.dataone.cn.indexer.parser;

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.w3c.dom.Document;

public class TemporalPeriodSolrField extends SolrField implements ISolrField {

    private static final String BEGIN_FIELD_NAME = "beginDate";
    private static final String END_FIELD_NAME = "endDate";
        
    public TemporalPeriodSolrField() {
    }

    public TemporalPeriodSolrField(String xpath) {
        this.xpath = xpath;
    }
    
    @Override
    public List<SolrElementField> getFields(Document doc, String identifier) throws Exception {
        
        String textValue = null;
        try {
            textValue = (String) xPathExpression.evaluate(doc, XPathConstants.STRING);
            if (textValue != null)
                textValue = textValue.trim();
        } catch (XPathExpressionException e) {
            throw new AssertionError("Unable to get temoral element string value.", e);
        }
        
        String beginFieldValue = "the begin date";
        String endFieldValue = "the end date";
        
        
        // TODO extract begin / end values from the textValue
        // see: http://dublincore.org/documents/dcmi-period 
        SolrDateConverter dateConverter = new SolrDateConverter();
        beginFieldValue = dateConverter.convert("2000");
        endFieldValue = dateConverter.convert("2000");
        
        
        
        
        
        
        List<SolrElementField> fields = new ArrayList<SolrElementField>();
        
        SolrElementField beginField = new SolrElementField();
        beginField.setName(BEGIN_FIELD_NAME);
        beginField.setValue(beginFieldValue);
        fields.add(beginField);
        
        SolrElementField endField = new SolrElementField();
        endField.setName(END_FIELD_NAME);
        endField.setValue(endFieldValue);
        fields.add(endField);
        
        return fields;
    }

}
