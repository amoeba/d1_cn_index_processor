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

package org.dataone.cn.index.processor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.client.v2.formats.ObjectFormatCache;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.index.util.PerformanceLogger;
import org.dataone.cn.indexer.XmlDocumentUtility;
import org.dataone.cn.indexer.resourcemap.ForesiteResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceMap;
import org.dataone.cn.indexer.resourcemap.ResourceMapFactory;
import org.dataone.cn.indexer.solrhttp.HTTPService;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.configuration.Settings;
import org.dataone.service.exceptions.BaseException;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.ObjectFormatIdentifier;
import org.dataone.service.types.v2.ObjectFormat;
import org.dataone.service.types.v2.SystemMetadata;
import org.dspace.foresite.OREParserException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.hibernate3.HibernateOptimisticLockingFailureException;
import org.w3c.dom.Document;

/**
 * IndexTaskProcessor is the controller class for processing IndexTasks. These
 * tasks are generated by the IndexTaskGenerator class and associated
 * collaborators. IndexTaskProcessor uses the IndexTaskRepository to locate
 * IndexTasks for processing and delegates to IndexTaskProcessingStrategy
 * implementations for actual processing behavior.
 * 
 * @author sroseboo
 * 
 */
public class IndexTaskProcessor {

    private static Logger logger = Logger.getLogger(IndexTaskProcessor.class.getName());
    private static final String FORMAT_TYPE_DATA = "DATA";
    private static final String LOAD_LOGGER_NAME = "indexProcessorLoad";
    private static int BATCH_UPDATE_SIZE = Settings.getConfiguration().getInt("dataone.indexing.batchUpdateSize", 1000);
    
    @Autowired
    private IndexTaskRepository repo;

    @Autowired
    private IndexTaskProcessingStrategy deleteProcessor;

    @Autowired
    private IndexTaskProcessingStrategy updateProcessor;

    @Autowired
    private HTTPService httpService;

    @Autowired
    private String solrQueryUri;

    private PerformanceLogger perfLog = PerformanceLogger.getInstance();
    
    public IndexTaskProcessor() {
    }

    /**
     * Processes the index task queue written by the IndexTaskGenerator, 
     * but unlike {@link #processIndexTaskQueue()}, all IndexTasks that 
     * add solr documents will be grouped into batches and done in one
     * command to solr.
     */
    public void batchProcessIndexTaskQueue() {
        logProcessorLoad();
        
        List<IndexTask> queue = getIndexTaskQueue();
        List<IndexTask> batchProcessList = new ArrayList<IndexTask>(BATCH_UPDATE_SIZE);
        
        IndexTask nextTask = getNextIndexTask(queue);
        while (nextTask != null) {
            batchProcessList.add(nextTask);
            nextTask = getNextIndexTask(queue);
            
            if (batchProcessList.size() >= BATCH_UPDATE_SIZE) {
                batchProcessTasks(batchProcessList);
                batchProcessList = new ArrayList<IndexTask>(BATCH_UPDATE_SIZE);
            }
        }
        batchProcessTasks(batchProcessList);
        
        List<IndexTask> retryQueue = getIndexTaskRetryQueue();
        List<IndexTask> batchProcessRetryList = new ArrayList<IndexTask>(BATCH_UPDATE_SIZE);
        
        nextTask = getNextIndexTask(retryQueue);
        while (nextTask != null) {
            batchProcessRetryList.add(nextTask);
            nextTask = getNextIndexTask(retryQueue);
            
            if (batchProcessRetryList.size() >= BATCH_UPDATE_SIZE) {
                batchProcessTasks(batchProcessRetryList);
                batchProcessRetryList = new ArrayList<IndexTask>(BATCH_UPDATE_SIZE);
            }
        }
        batchProcessTasks(batchProcessRetryList);
    }
    
    /**
     * Start a round of IndexTask processing. The IndexTask data store is
     * abstracted as a queue of tasks to process ordered by priority and
     * modification date. Typically invoked periodically by a quartz scheduled
     * job.
     */
    public void processIndexTaskQueue() {
        logProcessorLoad();
        
        List<IndexTask> queue = getIndexTaskQueue();
        IndexTask task = getNextIndexTask(queue);
        while (task != null) {
            processTask(task);
            task = getNextIndexTask(queue);
        }

        List<IndexTask> retryQueue = getIndexTaskRetryQueue();
        task = getNextIndexTask(retryQueue);
        while (task != null) {
            processTask(task);
            task = getNextIndexTask(queue);
        }
    }

    /**
     * Logs the number of {@link IndexTask}s that need to be processed
     * and the number of tasks that have failed.
     */
    private void logProcessorLoad() {
        
        Logger loadLogger = Logger.getLogger(LOAD_LOGGER_NAME);
        
        Long newTasks = null;
        Long failedTasks = null;
        try {
            newTasks = repo.countByStatus(IndexTask.STATUS_NEW);
            failedTasks = repo.countByStatus(IndexTask.STATUS_FAILED);
        } catch (Exception e) {
            logger.error("Unable to count NEW or FAILED tasks in task index repository.", e);
        }
        
        loadLogger.info("new:" + newTasks + ", failed: " + failedTasks );
    }

    private void processTask(IndexTask task) {
        try {
            if (task.isDeleteTask()) {
                logger.info("Indexing delete task for pid: " + task.getPid());
                deleteProcessor.process(task);
            } else {
                logger.info("Indexing update task for pid: " + task.getPid());
                updateProcessor.process(task);
            }
        } catch (Exception e) {
            logger.error("Unable to process task for pid: " + task.getPid(), e);
            handleFailedTask(task);
            return;
        }
        repo.delete(task);
        logger.info("Indexing complete for pid: " + task.getPid());
    }

    private void batchProcessTasks(List<IndexTask> taskList) {
        
        List<IndexTask> updateTasks = new ArrayList<>();
        List<IndexTask> deleteTasks = new ArrayList<>();
        
        for (IndexTask task : taskList) {
            if (task.isDeleteTask()) {
                logger.info("Adding delete task to be processed for pid: " + task.getPid());
                deleteTasks.add(task);
            } else {
                logger.info("Adding update task to be processed for pid: " + task.getPid());
                updateTasks.add(task);
            }
        }    
        
        try {
            deleteProcessor.process(deleteTasks);
            
            for (IndexTask task : deleteTasks) {
                repo.delete(task);
                logger.info("Indexing complete for pid: " + task.getPid());
            }
            
        } catch (Exception e) {
            StringBuilder failedPids = new StringBuilder(); 
            for (IndexTask task : deleteTasks)
                failedPids.append(task.getPid()).append(", ");
            logger.error("Unable to process tasks for pids: " + failedPids.toString(), e);
            handleFailedTasks(deleteTasks);
        }
        
        try {
            updateProcessor.process(updateTasks);
            
            for (IndexTask task : updateTasks) {
                repo.delete(task);
                logger.info("Indexing complete for pid: " + task.getPid());
            }
            
        } catch (Exception e) {
            StringBuilder failedPids = new StringBuilder(); 
            for (IndexTask task : updateTasks)
                failedPids.append(task.getPid()).append(", ");
            logger.error("Unable to process tasks for pids: " + failedPids.toString(), e);
            handleFailedTasks(deleteTasks);
        }
    }
    
    private void handleFailedTasks(List<IndexTask> tasks) {
        for (IndexTask task : tasks) {
            task.markFailed();
            saveTask(task);
        }
    }
    
    private void handleFailedTask(IndexTask task) {
        task.markFailed();
        saveTask(task);
    }

    private IndexTask getNextIndexTask(List<IndexTask> queue) {
        IndexTask task = null;
        while (task == null && queue.isEmpty() == false) {
            task = queue.remove(0);

            if (task == null)
                continue;
            
            task.markInProgress();
            task = saveTask(task);

            logger.info("Start of indexing pid: " + task.getPid());

            if (task != null && task.isDeleteTask()) {
                return task;
            }

            if (task != null && !isObjectPathReady(task)) {
                task.markNew();
                saveTask(task);
                logger.info("Task for pid: " + task.getPid() + " not processed.");
                task = null;
                continue;
            }

            if (task != null && !isResourceMapReadyToIndex(task, queue)) {
                task.markNew();
                saveTask(task);
                logger.info("Task for pid: " + task.getPid() + " not processed.");
                task = null;
                continue;
            }
        }
        return task;
    }

    private boolean isResourceMapReadyToIndex(IndexTask task, List<IndexTask> queue) {
        boolean ready = true;

        if (representsResourceMap(task)) {
            ResourceMap rm = null;
            try {
                rm = ResourceMapFactory.buildResourceMap(task.getObjectPath());
                List<String> referencedIds = rm.getAllDocumentIDs();
                referencedIds.remove(task.getPid());

                if (areAllReferencedDocsIndexed(referencedIds) == false) {
                    logger.info("Not all map resource references indexed for map: " + task.getPid()
                            + ".  Marking new and continuing...");
                    ready = false;
                }
            } catch (OREParserException oreException) {
                ready = false;
                logger.error("Unable to parse ORE doc: " + task.getPid()
                        + ".  Unrecoverable parse error: task will not be re-tried.");
                if (logger.isTraceEnabled()) {
                    oreException.printStackTrace();

                }
            } catch (Exception e) {
                ready = false;
                logger.error("unable to load resource for pid: " + task.getPid()
                        + " at object path: " + task.getObjectPath()
                        + ".  Marking new and continuing...");
            }
        }

        return ready;
    }

    private boolean areAllReferencedDocsIndexed(List<String> referencedIds) {
        if (referencedIds == null || referencedIds.size() == 0) {
            return true; // empty reference map...ok/ready to index.
        }
        List<SolrDoc> updateDocuments = null;
        int numberOfIndexedOrRemovedReferences = 0;
        try {
            updateDocuments = httpService.getDocumentsById(this.solrQueryUri, referencedIds);
            numberOfIndexedOrRemovedReferences = 0;
            for (String id : referencedIds) {
                boolean foundId = false;
                for (SolrDoc solrDoc : updateDocuments) {
                    if (solrDoc.getIdentifier().equals(id) || id.equals(solrDoc.getSeriesId())) {
                        foundId = true;
                        numberOfIndexedOrRemovedReferences++;
                        break;
                    }
                }
                if (foundId == false) {
                    Identifier pid = new Identifier();
                    pid.setValue(id);
                    logger.debug("Identifier " + id
                            + " was not found in the referenced id list in the Solr search index.");
                    SystemMetadata smd = HazelcastClientFactory.getSystemMetadataMap().get(pid);
                    if (smd != null && notVisibleInIndex(smd)) {
                        numberOfIndexedOrRemovedReferences++;
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return referencedIds.size() == numberOfIndexedOrRemovedReferences;
    }

    private boolean notVisibleInIndex(SystemMetadata smd) {
        return (!SolrDoc.visibleInIndex(smd) && smd != null);
    }

    private boolean representsResourceMap(IndexTask task) {
        return ForesiteResourceMap.representsResourceMap(task.getFormatId());
    }

    // if objectPath is not filled, attempt to fill it via hazelclient.
    // if object path is not available, do not process this task yet.
    // if object path is available, update task and process
    private boolean isObjectPathReady(IndexTask task) {
        boolean ok = true;
        boolean isDataObject = isDataObject(task);
        if (task.getObjectPath() == null && !isDataObject) {
            String objectPath = retrieveObjectPath(task.getPid());
            if (objectPath == null) {
                ok = false;
                evictObjectPathEntry(task.getPid());
                logger.info("Object path for pid: " + task.getPid()
                        + " is not available.  Object path entry will be evicting from map.  "
                        + "Task will be retried.");
            }
            task.setObjectPath(objectPath);
        }

        if (task.getObjectPath() != null && !isDataObject) {
            File objectPathFile = new File(task.getObjectPath());
            if (!objectPathFile.exists()) {
                // object path is present but doesnt correspond to a file
                // this task is not ready to index.
                ok = false;
                logger.info("Object path exists for pid: " + task.getPid()
                        + " however the file location: " + task.getObjectPath()
                        + " does not exist.  "
                        + "Marking not ready - task will be marked new and retried.");
            }
        }
        return ok;
    }

    private boolean isDataObject(IndexTask task) {
        ObjectFormat format = null;
        try {
            ObjectFormatIdentifier formatId = new ObjectFormatIdentifier();
            formatId.setValue(task.getFormatId());
            format = ObjectFormatCache.getInstance().getFormat(formatId);
        } catch (BaseException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return FORMAT_TYPE_DATA.equals(format.getFormatType());
    }

    private String retrieveObjectPath(String pid) {
        Identifier PID = new Identifier();
        PID.setValue(pid);
        return HazelcastClientFactory.getObjectPathMap().get(PID);
    }

    private void evictObjectPathEntry(String pid) {
        Identifier PID = new Identifier();
        PID.setValue(pid);
        HazelcastClientFactory.getObjectPathMap().evict(PID);
    }

    private List<IndexTask> getIndexTaskQueue() {
        long getIndexTasksStart = System.currentTimeMillis();
        List<IndexTask> indexTasks = repo.findByStatusOrderByPriorityAscTaskModifiedDateAsc(IndexTask.STATUS_NEW);
        perfLog.log("IndexTaskProcessor.getIndexTaskQueue() fetching NEW IndexTasks from repo", System.currentTimeMillis() - getIndexTasksStart);
        return indexTasks;
    }

    private List<IndexTask> getIndexTaskRetryQueue() {
        return repo.findByStatusAndNextExecutionLessThan(IndexTask.STATUS_FAILED,
                System.currentTimeMillis());
    }

    private IndexTask saveTask(IndexTask task) {
        try {
            task = repo.save(task);
        } catch (HibernateOptimisticLockingFailureException e) {
            logger.error("Unable to update index task for pid: " + task.getPid() + ".");
            task = null;
        }
        return task;
    }

    private Document loadDocument(IndexTask task) {
        Document docObject = null;
        try {
            docObject = XmlDocumentUtility.loadDocument(task.getObjectPath());
        } catch (Exception e) {
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
