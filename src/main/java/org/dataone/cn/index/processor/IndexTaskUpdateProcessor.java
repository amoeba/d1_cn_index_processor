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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.List;

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientFactory;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.cn.indexer.SolrIndexServiceV2;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v2.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.springframework.beans.factory.annotation.Autowired;
import org.xml.sax.SAXParseException;

public class IndexTaskUpdateProcessor implements IndexTaskProcessingStrategy {

    private static Logger logger = Logger.getLogger(IndexTaskUpdateProcessor.class.getName());

    @Autowired
    private SolrIndexService solrIndexService;
    
    @Autowired
    private SolrIndexServiceV2 solrIndexServiceV2;
    
    
    public void process(IndexTask task) throws Exception {
        InputStream smdStream = new ByteArrayInputStream(task.getSysMetadata().getBytes());
        try {
//            solrIndexService.insertIntoIndex(task.getPid(), smdStream, task.getObjectPath());
            solrIndexServiceV2.insertIntoIndex(task.getPid(), smdStream, task.getObjectPath());
        } catch (SAXParseException spe) {
            logger.error(spe);
            logger.error("Caught SAX parse exception on: " + task.getPid()
                    + ". re-trying with fresh copy of system metadata.");
            Identifier pid = new Identifier();
            pid.setValue(task.getPid());
            SystemMetadata smd = HazelcastClientFactory.getSystemMetadataMap().get(pid);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(smd, os);
            solrIndexService.insertIntoIndex(task.getPid(),
                    new ByteArrayInputStream(os.toByteArray()), task.getObjectPath());
            logger.error("Retry with fresh system metadata successful!");
        }
    }

    @Override
    public void process(List<IndexTask> tasks) throws Exception {
        try {
            solrIndexService.insertIntoIndex(tasks);
        } catch (SAXParseException spe) {
            logger.error(spe);
            StringBuilder failedPids = new StringBuilder(); 
            for (IndexTask task : tasks)
                failedPids.append(task.getPid()).append(", ");
            logger.error("Caught SAX parse exception on: " + failedPids + ". Unable to insert into index.");
        }
    }

}
