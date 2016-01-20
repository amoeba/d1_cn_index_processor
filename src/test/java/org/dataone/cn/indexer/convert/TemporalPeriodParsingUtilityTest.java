package org.dataone.cn.indexer.convert;

import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.dataone.cn.indexer.parser.utility.TemporalPeriodParsingUtility;
import org.junit.Test;

public class TemporalPeriodParsingUtilityTest {

    private TemporalPeriodParsingUtility temporalParsingtUtil = new TemporalPeriodParsingUtility();
    private SolrDateConverter dateConverter = new SolrDateConverter();
    
    @Test
    public void testGetScheme() {
        assertTrue(null == temporalParsingtUtil.getScheme(""));
        assertTrue(null == temporalParsingtUtil.getScheme("start=2000; end=2005;"));
        assertTrue("W3C-DTF".equals(temporalParsingtUtil.getScheme("scheme=W3C-DTF;")));
        assertTrue("W3C DTF".equals(temporalParsingtUtil.getScheme("scheme=W3C DTF;")));
        assertTrue("W3C".equals(temporalParsingtUtil.getScheme("scheme=W3C; DTF;")));
        assertTrue("W3C-DTF".equals(temporalParsingtUtil.getScheme("start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;")));
        assertTrue("W3C-DTF".equals(temporalParsingtUtil.getScheme("scheme=W3C-DTF; start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00;")));
    }
    
    @Test
    public void testGetStartDate() {
        
        Assert.assertEquals(null,
                temporalParsingtUtil.getFormattedStartDate("start=2000; end=2005;", "BAD_SCHEME"));
        
        Assert.assertEquals(dateConverter.convert("2000-01-01T07:00:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate("start=2000; end=2005;", null));
        Assert.assertEquals(dateConverter.convert("2000-08-01T06:00:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate("start=2000-08; end=2005;", null));
        Assert.assertEquals(dateConverter.convert("2000-08-02T06:00:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate("start=2000-08-02; end=2005;", null));
        
        Assert.assertEquals(dateConverter.convert("1999-09-25T04:20:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate(
                        "start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;",
                        "W3C-DTF"));
        Assert.assertEquals(dateConverter.convert("1999-09-25T04:20:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate(
                        "start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;",
                        ""));
        Assert.assertEquals(dateConverter.convert("1999-09-25T04:20:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate(
                        "start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;",
                        null));
        Assert.assertEquals(dateConverter.convert("1999-09-25T04:20:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate(
                        "scheme=W3C-DTF; start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00;",
                        "W3C-DTF"));
        Assert.assertEquals(dateConverter.convert("1999-09-25T04:20:00.000Z"),
                temporalParsingtUtil.getFormattedStartDate(
                        "end=1999-09-25T16:40+10:00; start=1999-09-25T14:20+10:00;",
                        null));
        
        Assert.assertEquals(null,
                temporalParsingtUtil.getFormattedStartDate(
                        "end=1999-09-25T16:40+10:00;",
                        null));
    }
    
    @Test
    public void testGetEndDate() {
        
        Assert.assertEquals(null,
                temporalParsingtUtil.getFormattedEndDate("start=2000; end=2005;", "BAD_SCHEME"));
        
        Assert.assertEquals(dateConverter.convert("2005-01-01T07:00:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate("end=2005;", null));
        Assert.assertEquals(dateConverter.convert("2005-03-01T07:00:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate("end=2005-03; start=2000;", null));
        Assert.assertEquals(dateConverter.convert("2005-03-02T07:00:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate("end=2005-03-02;", null));
        
        Assert.assertEquals(dateConverter.convert("1999-09-25T06:40:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate(
                        "start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;",
                        "W3C-DTF"));
        Assert.assertEquals(dateConverter.convert("1999-09-25T06:40:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate(
                        "start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;",
                        ""));
        Assert.assertEquals(dateConverter.convert("1999-09-25T06:40:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate(
                        "start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00; scheme=W3C-DTF;",
                        null));
        Assert.assertEquals(dateConverter.convert("1999-09-25T06:40:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate(
                        "scheme=W3C-DTF; start=1999-09-25T14:20+10:00; end=1999-09-25T16:40+10:00;",
                        "W3C-DTF"));
        Assert.assertEquals(dateConverter.convert("1999-09-25T06:40:00.000Z"),
                temporalParsingtUtil.getFormattedEndDate(
                        "end=1999-09-25T16:40+10:00; start=1999-09-25T14:20+10:00;",
                        null));
        
        Assert.assertEquals(null,
                temporalParsingtUtil.getFormattedEndDate(
                        "start=1999-09-25T16:40+10:00;",
                        null));
    }
}
