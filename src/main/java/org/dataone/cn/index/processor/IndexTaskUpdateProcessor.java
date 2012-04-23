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
import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.dataone.cn.hazelcast.HazelcastClientInstance;
import org.dataone.cn.index.task.IndexTask;
import org.dataone.cn.index.task.IndexTaskRepository;
import org.dataone.cn.indexer.XPathDocumentParser;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.SystemMetadata;
import org.dataone.service.util.TypeMarshaller;
import org.springframework.beans.factory.annotation.Autowired;
import org.xml.sax.SAXParseException;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.IMap;

public class IndexTaskUpdateProcessor implements IndexTaskProcessingStrategy {

    private static Logger logger = Logger.getLogger(IndexTaskUpdateProcessor.class.getName());

    @Autowired
    private ArrayList<XPathDocumentParser> documentParsers;

    @Autowired
    private IndexTaskRepository repo;

    private HazelcastClient hzClient;
    private IMap<Identifier, SystemMetadata> systemMetadata;
    private static final String HZ_SYSTEM_METADATA = Settings.getConfiguration().getString(
            "dataone.hazelcast.systemMetadata");

    public void process(IndexTask task) throws Exception {
        XPathDocumentParser parser = getXPathDocumentParser();
        InputStream smdStream = new ByteArrayInputStream(task.getSysMetadata().getBytes());

        try {
            parser.process(task.getPid(), smdStream, task.getObjectPath());
        } catch (SAXParseException spe) {
            logger.error(spe);
            logger.error("Caught SAX parse exception on: " + task.getPid()
                    + ". re-trying with fresh copy of system metadata.");
            startHazelClient();
            Identifier pid = new Identifier();
            pid.setValue(task.getPid());
            SystemMetadata smd = this.systemMetadata.get(pid);
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            TypeMarshaller.marshalTypeToOutputStream(smd, os);
            parser.process(task.getPid(), new ByteArrayInputStream(os.toByteArray()),
                    task.getObjectPath());
            logger.error("Retry with fresh system metadata successful!");
        }

    }

    private void startHazelClient() {
        if (this.hzClient == null) {
            this.hzClient = HazelcastClientInstance.getHazelcastClient();
            this.systemMetadata = this.hzClient.getMap(HZ_SYSTEM_METADATA);
        }
    }

    private XPathDocumentParser getXPathDocumentParser() {
        return documentParsers.get(0);
    }
}
