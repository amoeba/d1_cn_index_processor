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

import java.util.List;

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
public class IndexTaskProcessorJob implements InterruptableJob {

    private static Logger logger = Logger.getLogger(IndexTaskProcessorJob.class.getName());

    private static ApplicationContext context;
    private static IndexTaskProcessor processor;

    public IndexTaskProcessorJob() {
    }

    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        logger.info("executing index task processor...");
        setContext();
        processor.processIndexTaskQueue();
        logger.info("...finished execution of index task processor");
    }

    private static void setContext() {
        if (context == null || processor == null) {
            context = new ClassPathXmlApplicationContext("processor-daemon-context.xml");
            processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
        }
    }

    
    @Override
    public void interrupt() throws UnableToInterruptJobException {
        interruptCurrent();
        
    }
    
    public static void interruptCurrent() {
        processor.shutdownExecutor();
        
    }

    
    
    /**
     * Interrupt will call shutdown on the processor's executor service.
     * This will let executing tasks complete, but effectively cancels the 
     * rest of the submitted tasks (which can be quite a long list)
     * 
     * @throws UnableToInterruptJobException
     */
//    @Override
//    public void interrupt() throws UnableToInterruptJobException {
//        
//        try {
//            if (processor != null) {
//                processor.getExecutorService().shutdown(); 
//                // shutdown allows previously submitted tasks to finish executing
//                // but abandons submitted tasks waiting for execution.
//                // this leaves these tasks in "IN PROCESS" state.
//
//                // return those tasks stuck in process to new status
//                processor.resetTasksInProcessToNew();
//              //  processor.getExecutorService().
//
//
//            }
//        } catch (Throwable t) {
//            UnableToInterruptJobException e = new UnableToInterruptJobException(
//                    "Unable to shutdown the executorService that is processing index tasks."
//                    );
//            e.initCause(t);
//            throw e;
//        }
//    }
}
