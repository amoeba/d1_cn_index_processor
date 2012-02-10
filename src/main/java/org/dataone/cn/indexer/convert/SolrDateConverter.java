package org.dataone.cn.indexer.convert;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
  * User: Porter
 * Date: 7/26/11
 * Time: 6:04 PM
 */

/**Converts date to solr consumable format.
 *
 */

public class SolrDateConverter implements IConverter{

    /**Converts String data to solr consumable date format
     *
     */


    private static TimeZone OUTPUT_TIMEZONE = TimeZone.getTimeZone("Zulu");

//    SOLR FORMAT 1995-12-31T23:59:59Z
//    NOT 2005-07-21T14:55:30-0600
    /**solr date format
     *
     * */
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
        Calendar cal = javax.xml.bind.DatatypeConverter.parseDateTime(data);
        SimpleDateFormat sdf = new SimpleDateFormat(OUTPUT_DATE_FORMAT);
        sdf.setTimeZone(OUTPUT_TIMEZONE);
        String outputDateFormat = sdf.format(cal.getTime());

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
