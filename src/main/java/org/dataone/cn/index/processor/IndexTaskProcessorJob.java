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

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

@DisallowConcurrentExecution
public class IndexTaskProcessorJob implements Job {

    private static Logger logger = Logger.getLogger(IndexTaskProcessorJob.class.getName());

    private static ApplicationContext context;
    private static IndexTaskProcessor processor;

    public IndexTaskProcessorJob() {
    }

    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        logger.info("executing index task processor...");
        setContext();
        processor.processIndexTaskQueue();
    }

    private static void setContext() {
        if (context == null || processor == null) {
            context = new ClassPathXmlApplicationContext("processor-daemon-context.xml");
            processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
        }
    }
}
