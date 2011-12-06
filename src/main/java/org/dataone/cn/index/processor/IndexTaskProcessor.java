package org.dataone.cn.index.processor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.codec.EncoderException;
import org.apache.log4j.Logger;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;
import org.xml.sax.SAXException;

public class IndexTaskProcessor {

    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    private static Logger logger = Logger.getLogger(IndexTaskProcessor.class.getName());

    private static final String RESOURCE_MAP_FORMAT = "http://www.openarchives.org/ore/terms";

    public void processIndexTaskQueue() {
        IndexTask task = getNextIndexTask();
        while (task != null) {
            processIndexTask(task);
            task = getNextIndexTask();
        }
    }

    // TODO: handle resource maps - find out if all referenced objects available
    // to index, else put back in queue
    public void processIndexTask(IndexTask task) {
        XPathDocumentParser parser = getXPathDocumentParser();
        try {
            InputStream smdStream = new ByteArrayInputStream(task.getSysMetadata().getBytes());
            SolrDoc doc = parser.process(task.getPid(), smdStream, task.getObjectPath());
            repo.delete(task);
        } catch (XPathExpressionException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (SAXException e) {
            logger.error(e.getMessage(), e);
        } catch (ParserConfigurationException e) {
            logger.error(e.getMessage(), e);
        } catch (EncoderException e) {
            logger.error(e.getMessage(), e);
        }

    }

    private IndexTask getNextIndexTask() {
        List<IndexTask> queue = getIndexTaskQueue();
        int queueIndex = 0;
        IndexTask task = null;
        while (task == null && queueIndex < queue.size()) {
            task = queue.get(queueIndex++);
            task.markInProgress();
            try {
                task = repo.save(task);
            } catch (HibernateOptimisticLockingFailureException e) {
                logger.debug("Unable to update index task for pid: " + task.getPid()
                        + " prior to processing.  Moving to next taskin queue.");
                task = null;
            }
        }
        return task;
    }

    /*
     * Dealing with resource maps - push to top of queue by sorting on
     * resourceMap or by assigning resource maps a high priority. At the top,
     * best chance to short circuit repetitious update to solr index.
     */
    public List<IndexTask> getIndexTaskQueue() {
        return repo.findIndexTaskQueue(IndexTask.STATUS_NEW);
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }
}