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

package org.dataone.cn.indexer.convert;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**Converts date to solr consumable format.
 * User: Porter
 * Date: 7/26/11
 * Time: 6:04 PM
 */

public class SolrDateConverter implements IConverter{

    private static TimeZone OUTPUT_TIMEZONE = TimeZone.getTimeZone("Zulu");

    protected static final String OUTPUT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**Converts date to solr consumable format.
     *
     * All date times are converted to Zulu time zone with Millisecond accuracy.
     * http://lucene.apache.org/solr/api/org/apache/solr/schema/DateField.html
     *
     * @param  data See documentation for javax.xml.bind.DatatypeConverter.parseDateTime(data) for acceptable input formats.
     * @return Solr date format
     */
    public String convert(String data) {
        if(data == null || data.equals("")){
            return "";
        }
        //Date dateTime = DateTimeMarshaller.deserializeDateToUTC(data);
        String outputDateFormat = "";
        try {
            Date dateTime = javax.xml.bind.DatatypeConverter.parseDate(data).getTime();
            SimpleDateFormat sdf = new SimpleDateFormat(OUTPUT_DATE_FORMAT);
            sdf.setTimeZone(OUTPUT_TIMEZONE);
            outputDateFormat = sdf.format(dateTime);
        } catch (IllegalArgumentException iae) {
        }
        return outputDateFormat;
    }

    public static Date ParseSolrDate(String date) {
        Date outputDate = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(OUTPUT_DATE_FORMAT);
            outputDate = sdf.parse(date);
        } catch (ParseException e) {
            return null;
        }
        return outputDate;
    }

}
