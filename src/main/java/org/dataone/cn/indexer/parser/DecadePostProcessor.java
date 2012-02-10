package org.dataone.cn.indexer.parser;

import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * User: Porter
 */
public class DecadePostProcessor extends AbstractPostProcessor implements IPostProcessor {

    public Map<String, SolrDoc> process(String identifier, Map<String, SolrDoc> docMap) {

        for (SolrDoc solrDoc : docMap.values()) {

            if(!match(solrDoc)){
                continue;
            }
            String beginDate = solrDoc.getFirstFieldValue(SolrElementField.FIELD_BEGIN_DATE);
            String endDate = solrDoc.getFirstFieldValue(SolrElementField.FIELD_END_DATE);

            Date beginDateValue = beginDate==null||beginDate.length()<=0?null:SolrDateConverter.ParseSolrDate(beginDate);
            Date endDateValue = endDate==null||endDate.length()<=0?null:SolrDateConverter.ParseSolrDate(endDate);


            Calendar beginDateCal = null;
            Calendar endDateCal = null;
            if(beginDateValue != null){
               beginDateCal =  Calendar.getInstance();
                beginDateCal.setTime(beginDateValue);
            }if(endDateValue != null){
               endDateCal =  Calendar.getInstance();
                endDateCal.setTime(endDateValue);
            }

            if(endDateCal != null){
                int year = endDateCal.get(Calendar.YEAR);
                String decadeString = getDecadeString(year);
                solrDoc.updateOrAddField(SolrElementField.FIELD_DECADE, decadeString);
                continue;
            }else if(beginDateValue != null){
                int year = endDateCal.get(Calendar.YEAR);
                String decadeString = getDecadeString(year);
                solrDoc.updateOrAddField(SolrElementField.FIELD_DECADE, decadeString);
                continue;
            }
            else {
                solrDoc.updateOrAddField(SolrElementField.FIELD_DECADE, "Unknown");
            }

        }

        return docMap;
    }

    private String getDecadeString(int year) {
        int decadeBegin = year - (year % 10);
        int decadeEnd = year + 9;

        return decadeBegin + " to " + decadeEnd;
    }
}
