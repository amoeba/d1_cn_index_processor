package org.dataone.cn.index;

import org.apache.solr.common.SolrDocument;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.Resource;

/**
 * Solr unit test framework is dependent on JUnit 4.7. Later versions of junit
 * will break the base test classes.
 * 
 * @author sroseboo
 * 
 */
public class SolrIndexDeleteTest extends DataONESolrJettyTestBase {

    @Test
    public void testDeleteFromSolrIndex() throws Exception {

        String pid = "peggym.130.4";
        Resource systemMetadataResource = (Resource) context.getBean("peggym1304Sys");
        addToSolrIndex(systemMetadataResource);
        SolrDocument result = assertPresentInSolrIndex(pid);

        HTTPService httpService = (HTTPService) context.getBean("httpService");
        httpService.sendSolrDelete(pid, "UTF-8");

        Assert.assertTrue(getAllSolrDocuments().size() == 0);
    }
}
