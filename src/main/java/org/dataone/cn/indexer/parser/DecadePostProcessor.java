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
