package org.dataone.cn.indexer.convert;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FgdcDateConverter implements IConverter {

    private static TimeZone OUTPUT_TIMEZONE = TimeZone.getTimeZone("Zulu");
    private static final String OUTPUT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    private static final String DATE_PATTERNS[] = { "\\d{4}", // e.g. 1993
            "\\d{4}[01]\\d", // e.g. 199607
            "\\d{4}[01]\\d[0123]\\d", // e.g. 20000101 or 19981231
            "\\d{9}", // e.g. 196820405
            "\\d{4} onwards", // e.g. 1992 onwards
            "\\d{4} and \\d{4}", // e.g. 1989 and 1990
            "\\d{4}[/|-]\\d{4}", // e.g. 1995/1996 or 1991-1992
            "\\w* \\d{4}", // e.g. anyMonth 1999
            "\\w*, \\d{4}", // e.g. anyMonth, 1999
            "\\d{4} on", // e.g. 1980 on
            "\\d{4}-[01]\\d-[0123]\\d", // e.g. 2005-06-24
            "\\d{4}- \\[unpublished annual reports\\]" }; // e.g. 1990-
                                                          // [unpublished
                                                          // annual reports]
    private List<Pattern> patterns = new ArrayList<Pattern>();

    public FgdcDateConverter() {
        for (String datePattern : DATE_PATTERNS) {
            Pattern pattern = Pattern.compile(datePattern);
            patterns.add(pattern);
        }
    }

    public String convert(String data) {
        Date date = textToDate(data);
        SimpleDateFormat sdf = new SimpleDateFormat(OUTPUT_DATE_FORMAT);
        sdf.setTimeZone(OUTPUT_TIMEZONE);

        String outputDateFormat = sdf.format(date.getTime());

        return outputDateFormat;

    }

    public Date textToDate(String dateString) {

        Date convertedDate = null;
        Boolean validPattern = false;
        Boolean convertMonth = false;
        String defaultDay = "01";

        // Check for valid patterns
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(dateString);
            if (matcher.matches()) {
                validPattern = true;
                if (pattern.pattern().substring(0, 3).equals("\\w*")) {
                    convertMonth = true;
                }
                break;
            }
        }
        if (validPattern) {
            // If pattern is valid, then extract date
            int dateStringLen = dateString.length();

            String extractedYear = "";
            String extractedMonth = "";
            String extractedDay = "";

            // If pattern starts with month, convert month to number
            if (convertMonth) {
                extractedYear = dateString.substring(dateStringLen - 4, dateStringLen);

                SimpleDateFormat formatter = new SimpleDateFormat("MMM");
                try {
                    Date tempDate = formatter.parse(dateString.substring(0, 3));
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(tempDate);
                    extractedMonth = Integer.toString((cal.get(Calendar.MONTH)) + 1);
                } catch (ParseException e) {
                    // Not a valid month
                    return convertedDate;
                }
                extractedDay = defaultDay;
            } else {

                // Extract Date based on length of dateString
                switch (dateStringLen) {
                case 4: // e.g. 1993
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = "01";
                    extractedDay = defaultDay;
                    break;
                case 6: // e.g. 199607
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = dateString.substring(4, 6);
                    extractedDay = defaultDay;
                    break;
                case 7: // e.g. 1980 on
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = "01";
                    extractedDay = defaultDay;
                    break;
                case 8: // e.g. 20000101, 19981231
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = dateString.substring(4, 6);
                    extractedDay = dateString.substring(6, 8);
                    break;
                case 9: // e.g. 1995/1996 or 196820405
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = "01";
                    extractedDay = defaultDay;
                    break;
                case 10: // e.g. 2005-06-24
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = dateString.substring(5, 7);
                    extractedDay = dateString.substring(8, 10);
                    break;
                case 12: // e.g. 1992 onwards
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = "01";
                    extractedDay = defaultDay;
                    break;
                case 13: // e.g. 1989 and 1990
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = "01";
                    extractedDay = defaultDay;
                    break;
                case 34: // e.g. 1990- [unpublished annual reports]
                    extractedYear = dateString.substring(0, 4);
                    extractedMonth = "01";
                    extractedDay = defaultDay;
                    break;
                }
            }
            String extractedDate = extractedYear + "-" + extractedMonth + "-" + extractedDay
                    + " 00:00:00+0000";

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
            formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

            try {
                convertedDate = formatter.parse(extractedDate);
            } catch (ParseException e) {
                return convertedDate;
            }
        }
        return convertedDate;
    }

}
