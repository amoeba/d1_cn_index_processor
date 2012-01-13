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
import org.dataone.client.ObjectFormatCache;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.NotFound;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class IndexTaskProcessor {

    private static Logger logger = Logger.getLogger(IndexTaskProcessor.class.getName());

    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    @Autowired
    private HTTPService httpService;

    private static final String FORMAT_TYPE_DATA = "DATA";
    private static final String RESOURCE_MAP_FORMAT = "http://www.openarchives.org/ore/terms";
    private static final String CHAR_ENCODING = "UTF-8";

    private HazelcastInstance hzClient;

    private static final String HZ_OBJECT_PATH = Settings.getConfiguration().getString(
            "dataone.hazelcast.objectPath");

    private String solrQueryUri;

    private IMap<Identifier, String> objectPaths;

    public IndexTaskProcessor() {
    }

    public void processIndexTaskQueue() {
        startHazelClient();
        List<IndexTask> queue = getIndexTaskQueue();
        IndexTask task = getNextIndexTask(queue);
        while (task != null) {
            processAddUpdateIndexTask(task);
            task = getNextIndexTask(queue);
        }
    }

    private void processAddUpdateIndexTask(IndexTask task) {
        XPathDocumentParser parser = getXPathDocumentParser();
        InputStream smdStream = new ByteArrayInputStream(task.getSysMetadata().getBytes());
        try {
            parser.process(task.getPid(), smdStream, task.getObjectPath());
        } catch (XPathExpressionException e) {
            logger.error(e.getMessage(), e);
            handleFailedTask(task);
            return;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            handleFailedTask(task);
            return;
        } catch (SAXException e) {
            logger.error(e.getMessage(), e);
            handleFailedTask(task);
            return;
        } catch (ParserConfigurationException e) {
            logger.error(e.getMessage(), e);
            handleFailedTask(task);
            return;
        } catch (EncoderException e) {
            logger.error(e.getMessage(), e);
            handleFailedTask(task);
            return;
        }
        repo.delete(task);
    }

    private IndexTask getNextIndexTask(List<IndexTask> queue) {
        IndexTask task = null;
        while (task == null && queue.isEmpty() == false) {
            task = queue.remove(0);

            task.markInProgress();
            task = saveTask(task);

            if (task != null && task.isDeleteTask()) {
                processDeleteIndexTask(task);
                repo.delete(task);
                task = null;
                continue;
            }

            if (task != null && !isObjectPathReady(task)) {
                task.markNew();
                saveTask(task);
                task = null;
                continue;
            }

            if (task != null && !isResourceMapReadyToIndex(task, queue)) {
                task = null;
            }
        }
        return task;
    }

    private void processDeleteIndexTask(IndexTask task) {
        httpService.sendSolrDelete(task.getPid(), CHAR_ENCODING);
    }

    private boolean isResourceMapReadyToIndex(IndexTask task, List<IndexTask> queue) {
        boolean ready = true;
        if (representsResourceMap(task)) {
            Document docObject = loadDocument(task);
            if (docObject == null) {
                logger.debug("unable to load resource at object path: " + task.getObjectPath()
                        + ".  Marking new and continuing...");
                task.markNew();
                repo.save(task);
                ready = false;
            } else if (docObject != null) {
                ResourceMap rm = null;
                try {
                    rm = new ResourceMap(docObject);
                } catch (XPathExpressionException e) {
                    logger.error(e.getMessage(), e);
                }
                List<String> referencedIds = rm.getAllDocumentIDs();
                referencedIds.remove(task.getPid());
                if (areAllReferencedDocsIndexed(referencedIds) == false) {
                    logger.info("Not all map resource references indexed for map: " + task.getPid()
                            + ".  Marking new and continuing...");
                    task.markNew();
                    saveTask(task);
                    ready = false;
                }
            }
        }
        return ready;
    }

    private boolean areAllReferencedDocsIndexed(List<String> referencedIds) {
        List<SolrDoc> updateDocuments = null;
        try {
            updateDocuments = httpService.getDocuments(this.solrQueryUri, referencedIds);
        } catch (XPathExpressionException e) {
            logger.error(e.getMessage(), e);
            return false;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        } catch (EncoderException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return updateDocuments != null && referencedIds.size() == updateDocuments.size();
    }

    private boolean representsResourceMap(IndexTask task) {
        return RESOURCE_MAP_FORMAT.equals(task.getFormatId());
    }

    // if objectPath is not filled, attempt to fill it via hazelclient.
    // if object path is not available, do not process this task yet.
    // if object path is available, update task and process
    private boolean isObjectPathReady(IndexTask task) {
        boolean ok = true;
        if (task.getObjectPath() == null && !isDataObject(task)) {
            String objectPath = retrieveObjectPath(task.getPid());
            if (objectPath == null) {
                ok = false;
            }
            task.setObjectPath(objectPath);
        }
        return ok;
    }

    private boolean isDataObject(IndexTask task) {
        ObjectFormat format = null;
        try {
            format = ObjectFormatCache.getInstance().getFormat(task.getFormatId());
        } catch (NotFound e) {
            e.printStackTrace();
            return false;
        }
        return FORMAT_TYPE_DATA.equals(format.getFormatType());
    }

    private String retrieveObjectPath(String pid) {
        Identifier PID = new Identifier();
        PID.setValue(pid);
        return objectPaths.get(PID);
    }

    private void startHazelClient() {
        if (this.hzClient == null) {
            this.hzClient = HazelcastClientInstance.getHazelcastClient();
            this.objectPaths = this.hzClient.getMap(HZ_OBJECT_PATH);
        }
    }

    private List<IndexTask> getIndexTaskQueue() {
        return repo.findByStatusOrderByPriorityAscTaskModifiedDateAsc(IndexTask.STATUS_NEW);
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }

    private void handleFailedTask(IndexTask task) {
        task.markFailed();
        saveTask(task);
    }

    private IndexTask saveTask(IndexTask task) {
        try {
            task = repo.save(task);
        } catch (HibernateOptimisticLockingFailureException e) {
            logger.debug("Unable to update index task for pid: " + task.getPid() + ".");
            task = null;
        }
        return task;
    }

    private Document loadDocument(IndexTask task) {
        Document docObject = null;
        try {
            docObject = getXPathDocumentParser().loadDocument(task.getObjectPath(), CHAR_ENCODING);
        } catch (ParserConfigurationException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (SAXException e) {
            logger.error(e.getMessage(), e);
        }
        if (docObject == null) {
            logger.error("Could not load OBJECT file for ID,Path=" + task.getPid() + ", "
                    + task.getObjectPath());
        }
        return docObject;
    }

    public void setSolrQueryUri(String uri) {
        this.solrQueryUri = uri;
    }
}