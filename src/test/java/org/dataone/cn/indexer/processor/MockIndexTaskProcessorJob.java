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
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;
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
public class MockIndexTaskProcessorJob implements InterruptableJob {

    private static Logger logger = Logger.getLogger(MockIndexTaskProcessorJob.class.getName());

    private static Date context;
    private static MockTaskProcessor processor = new MockTaskProcessor();

    public MockIndexTaskProcessorJob() {
        System.out.println("instantiating MockIndexTaskProcessorJob: ");
    }

    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        long start = System.currentTimeMillis();
        System.out.println("MockIndexTaskProcessorJob: entering execute... ");

        processor.processList();
        long end = System.currentTimeMillis();
        System.out.println("MockIndexTaskProcessorJob...finished execution in (millis) " + (end - start));//System.currentTimeMillis());
    }
    

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interruptCurrent();
        
    }
    
    public static void interruptCurrent() {
        System.out.println("********************* ProcessorJob interrupt called, calling executorservice shutdown...");
        processor.getExecutorService().shutdown();
    }
}
