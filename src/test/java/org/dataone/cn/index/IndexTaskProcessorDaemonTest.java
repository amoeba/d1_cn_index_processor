package org.dataone.cn.index;

import junit.framework.Assert;

import org.apache.log4j.Logger;
import org.dataone.cn.index.processor.IndexTaskProcessorDaemon;
import org.junit.Test;

public class IndexTaskProcessorDaemonTest {

    private static Logger logger = Logger.getLogger(IndexTaskProcessorDaemonTest.class.getName());

    @Test
    public void testDaemon() {
        IndexTaskProcessorDaemon daemon = new IndexTaskProcessorDaemon();
        try {
            daemon.start();
            Thread.sleep(10000);
            daemon.stop();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.assertTrue(false);
        }
        Assert.assertTrue(true);
    }

}
