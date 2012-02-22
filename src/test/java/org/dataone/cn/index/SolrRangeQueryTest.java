package org.dataone.cn.index;

import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.Resource;

public class SolrRangeQueryTest extends DataONESolrJettyTestBase {

    private Resource peggym1304Sys;

    @Test
    public void testSimpleRangeQuery() throws Exception {
        sendSolrDeleteAll();

        String pid = "peggym.130.4";
        peggym1304Sys = (Resource) context.getBean("peggym1304Sys");
        addToSolrIndex(peggym1304Sys);
        assertPresentInSolrIndex(pid);

        SolrDocumentList sdl = null;
        sdl = findByField("westBoundCoord", "[\\-130 TO 0 ]");
        Assert.assertEquals(1, sdl.size());
    }

    @Test
    public void testTwoFieldRangeQuery() throws Exception {
        sendSolrDeleteAll();

        String pid = "peggym.130.4";
        peggym1304Sys = (Resource) context.getBean("peggym1304Sys");
        addToSolrIndex(peggym1304Sys);
        assertPresentInSolrIndex(pid);

        SolrDocumentList sdl = null;
        sdl = findByQueryString("westBoundCoord:[\\-130 TO 0] AND northBoundCoord:[0 TO 30]");
        Assert.assertEquals(1, sdl.size());
    }

    @Test
    public void testFourFieldRangeQuery() throws Exception {
        sendSolrDeleteAll();

        String pid = "peggym.130.4";
        peggym1304Sys = (Resource) context.getBean("peggym1304Sys");
        addToSolrIndex(peggym1304Sys);
        assertPresentInSolrIndex(pid);

        SolrDocumentList sdl = null;
        sdl = findByQueryString("westBoundCoord:[\\-130 TO 0] " //
                + "AND southBoundCoord:[0 TO 30] " //
                + "AND eastBoundCoord:[\\-150 TO 20] " //
                + "AND northBoundCoord:[\\-10 TO 50]");
        Assert.assertEquals(1, sdl.size());
    }
}
