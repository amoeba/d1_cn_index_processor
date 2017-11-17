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
import org.quartz.InterruptableJob;
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
    private static int jobIdentityHash = System.identityHashCode(IndexTaskProcessorJob.class);

    public IndexTaskProcessorJob() {
    }

    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        logger.warn("processing job [" + jobIdentityHash + "/" + this + "] executing index task with processor [" + processor + "]");
        setContext();
        processor.processIndexTaskQueue();
        logger.warn("processing job [" + this + "] finished execution of index task processor [" + processor + "]");
    }

    private static void setContext() {
        if (context == null || processor == null) {
            context = new ClassPathXmlApplicationContext("processor-daemon-context.xml");
            processor = (IndexTaskProcessor) context.getBean("indexTaskProcessor");
        }
    }

    /**
     * Interrupt calls the held processor's executor service.
     * This will let executing tasks complete, but effectively cancels the 
     * rest of the submitted tasks (which can be quite a long list), and returns
     * 
     * 
     * @throws UnableToInterruptJobException
     */  
    @Override
    public void interrupt() throws UnableToInterruptJobException {
        logger.warn("IndexTaskProcessorJob [" + this + "] interrupted, shutting down processor [" + processor + "]");
        interruptCurrent();
    }
    
    /**
     * Interrupt calls the held processor's executor service.
     * This will let executing tasks complete, but effectively cancels the 
     * rest of the submitted tasks (which can be quite a long list), and returns
     * 
     * 
     * @throws UnableToInterruptJobException
     */
    public static void interruptCurrent() throws UnableToInterruptJobException {
        try {
            logger.warn("IndexTaskProcessorJob class [" + jobIdentityHash + "] interruptCurrent called, shutting down processor [" + processor + "]");
            processor.shutdownExecutor();
        } catch (Throwable t) {
            UnableToInterruptJobException e = new UnableToInterruptJobException(
                    "Unable to shutdown the executorService that is processing index tasks."
                    );
            e.initCause(t);
            throw e;
        }
        
    }

}