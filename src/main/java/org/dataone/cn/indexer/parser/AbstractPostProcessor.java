package org.dataone.cn.indexer.parser;

import org.dataone.cn.indexer.solrhttp.SolrDoc;

import java.util.List;

/**
 * User: Porter
 */
public class AbstractPostProcessor {
    protected String matchField = null;
    protected List<String> matchValue = null;

    public boolean match(SolrDoc solrDoc){
        String value = solrDoc.getFirstFieldValue(matchField);

        if(value == null){
            return false;
        }

        for (String valueToMatch : matchValue) {
            if(valueToMatch == null){
                continue;
            }
            else if(value.equals(valueToMatch)){
                return true;
            }
        }
        return false;
    }

    public String getMatchField() {
        return matchField;
    }

    public void setMatchField(String matchField) {
        this.matchField = matchField;
    }

    public List<String> getMatchValue() {
        return matchValue;
    }

    public void setMatchValue(List<String> matchValue) {
        this.matchValue = matchValue;
    }
}
