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

package org.dataone.cn.indexer.processor;

import java.util.Date;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * The scope of each job is processing a list of IndexTasks.  Only one Job is 
 * permitted to run at a time.
 * 
 * @author rnahf
 *
 */
@DisallowConcurrentExecution
public class MockIndexTaskProcessorJob implements Job {

    private static Logger logger = Logger.getLogger(MockIndexTaskProcessorJob.class.getName());

    private static Date context;
    private static MockIndexTaskProcessor processor;

    public MockIndexTaskProcessorJob() {
        System.out.println("instantiating MockIndexTaskProcessorJob: ");
    }

    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        long start = System.currentTimeMillis();
        System.out.println("executing index task processor... ");
        setContext();
        processor.mockProcessIndexTaskQueue();
        long end = System.currentTimeMillis();
        System.out.println("...finished execution of index task processor in (millis) " + (end - start));//System.currentTimeMillis());
    }

    private static void setContext() {
        System.out.println("Entering setContext...");
        if (context == null || processor == null) {
            context = new Date();
            try {
                processor = new MockIndexTaskProcessor();
            } catch (Exception e)
            {}
            System.out.println("Set Job context and processor: " + context + " " + processor);
            
        }
    }
}
