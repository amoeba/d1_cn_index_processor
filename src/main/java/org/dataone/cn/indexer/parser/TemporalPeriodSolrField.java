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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.convert.SolrDateConverter;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.w3c.dom.Document;

/**
 * Used to convert the text inside of a dcterms:temporal with xsi:type="dcterms:Period" 
 * into {@link SolrElementField}s for beginDate and endDate
 * Example input: 
 * <pre>
 * {@code
 * <dcterms:temporal xsi:type="dcterms:Period">
 *      start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;
 * </dcterms:temporal>
 * }
 * </pre>
 * Currently supports at least a W3C-DTF scheme, meaning the patterns specified here: 
 * <a href="https://www.w3.org/TR/NOTE-datetime">https://www.w3.org/TR/NOTE-datetime</a>
 * </br>
 * Actually supports a somewhat larger superset of those; see {@link ISODateTimeFormat#dateTimeParser}
 * 
 * @author Andrei
 */
public class TemporalPeriodSolrField extends SolrField implements ISolrField {

    private static Logger log = Logger.getLogger(TemporalPeriodSolrField.class);
    
    private static final String BEGIN_FIELD_NAME = "beginDate";
    private static final String END_FIELD_NAME = "endDate";
    private static final String W3C_DTF_SCHEME = "W3C-DTF";
    private static final Pattern START_PATTERN = Pattern.compile(".*start=(.*?);.*");
    private static final Pattern END_PATTERN = Pattern.compile(".*end=(.*?);.*");
    private static final Pattern SCHEME_PATTERN = Pattern.compile(".*scheme=(.*?);.*");

    private static final DateTimeFormatter[] FORMATTERS = {
        ISODateTimeFormat.dateTimeParser()
        // ^ actually supports a superset of W3C-DTF formats
        // to limit it to the stricter formats, replace the above with:
        //        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSZZ"),
        //        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ"),
        //        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSZZ"),
        //        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SZZ"),
        //        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZZ"),
        //        DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mmZZ"),
        //        DateTimeFormat.forPattern("yyyy-MM-dd"),
        //        DateTimeFormat.forPattern("yyyy-MM"),
        //        DateTimeFormat.forPattern("yyyy")
    };
    
    private IConverter dateConverter = new SolrDateConverter();
    
    
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
        
        Matcher startMatcher = START_PATTERN.matcher(textValue);
        Matcher endMatcher = END_PATTERN.matcher(textValue);
        Matcher schemeMatcher = SCHEME_PATTERN.matcher(textValue);
        
        String startString = null;
        String endString = null;
        String schemeString = null;
        
        if (startMatcher.find())
            startString = startMatcher.group(1);
        if (endMatcher.find())
            endString = endMatcher.group(1);
        if (schemeMatcher.find())
            schemeString = schemeMatcher.group(1);
        
        if (schemeString != null && !schemeString.equalsIgnoreCase(W3C_DTF_SCHEME))
            log.warn("Scheme \"" + schemeString + "\" may not be supported. "
                    + "Currently supporting: " + W3C_DTF_SCHEME);
        if (startString == null && endString == null)
            throw new AssertionError("Couldn't extract 'start' or 'end' date. "
                    + "Temporal pattern of type period needs to contain at least one of these. "
                    + "Value was: " + textValue);
        
        // convert from W3C-DTF (or ISO8601 subset)
        // to valid xsd:datetime needed for SolrDateConverter
        
        DateTime startDate = parseDateTime(startString);
        DateTime endDate = parseDateTime(endString);
        
        String schemeWarning = StringUtils.isEmpty(schemeString) ? "" : schemeString + " may not be supported. ";
        if (startString != null && startDate == null)
            throw new AssertionError("Start date string could not be parsed. "
                    + schemeWarning + "Start date string was: " + startString);
        if (endString != null && endDate == null)
            throw new AssertionError("End date string could not be parsed. "
                    + schemeWarning + "End date string was: " + endString);
        
        // if period only specifies the start OR end, we set that to both
        if (startDate != null && endDate == null)
            endDate = startDate;
        if (endDate != null && startDate == null)
            startDate = endDate;
        
        DateTimeFormatter iso8601Formatter = ISODateTimeFormat.dateTime();
        String startDateIso8601 = iso8601Formatter.print(startDate.toDateTime(DateTimeZone.UTC));
        String endDateIso8601 = iso8601Formatter.print(endDate.toDateTime(DateTimeZone.UTC));
        String beginFieldValue = dateConverter.convert(startDateIso8601);
        String endFieldValue = dateConverter.convert(endDateIso8601);
        
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

    private DateTime parseDateTime(String dateString) {
        DateTime dateTime = null;
        for (DateTimeFormatter formatter : FORMATTERS) {
            try {
                dateTime = formatter.parseDateTime(dateString);
                break;
            } catch (Exception e) {
                continue;
            }
        }
        return dateTime;
    }

}
