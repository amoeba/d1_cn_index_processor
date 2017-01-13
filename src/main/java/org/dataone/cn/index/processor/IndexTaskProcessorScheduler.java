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

import static org.quartz.JobBuilder.newJob;
import static org.quartz.JobKey.jobKey;
import static org.quartz.TriggerBuilder.newTrigger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;

public class IndexTaskProcessorScheduler {

    private static Logger logger = Logger.getLogger(IndexTaskProcessorScheduler.class.getName());

    private static final String QUARTZ_PROCESSOR_TRIGGER = "index-process-trigger";
    private static final String QUARTZ_PROCESSOR_GROUP = "d1-cn-index-processor";
    private static final String QUARTZ_PROCESSOR_JOB = "d1-index-processor-job";

    private Scheduler scheduler;

    /**
     * called by the IndexTaskProcessorDaemon's start method 
     */
    public void start() {
        try {
            logger.info("starting index task processor quartz scheduler....");
            Properties properties = new Properties();
            properties.load(this.getClass().getResourceAsStream(
                    "/org/dataone/configuration/quartz.properties"));
            SchedulerFactory schedulerFactory = new StdSchedulerFactory(properties);
            scheduler = schedulerFactory.getScheduler();

            JobDetail job = newJob(IndexTaskProcessorJob.class).withIdentity(QUARTZ_PROCESSOR_JOB,
                    QUARTZ_PROCESSOR_GROUP).build();

            SimpleTrigger trigger = newTrigger()
                    .withIdentity(QUARTZ_PROCESSOR_TRIGGER, QUARTZ_PROCESSOR_GROUP)//
                    .startNow() //
                    .withSchedule(SimpleScheduleBuilder.repeatMinutelyForever(2)) //
                    .build();

            scheduler.scheduleJob(job, trigger);
            scheduler.start();

        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    /** 
     * called by IndexTaskProcessorDaemon's stop method.  Puts the started scheduler
     * on standby, then waits for the scheduler's executing IndexTaskProcesssorJobs 
     * to finish.  Depending on the configuration of the Job, there may me more than
     * one executing at a time.  
     * TODO:  should we send an interrupt or stop to the job? 
     */
    public void stop() {
        logger.info("stopping index task processor quartz scheduler...");
        try {
            
            if (scheduler.isStarted()) {
                scheduler.standby();  // this stops execution and triggering
                                      // keeping backlogged triggers from executing
                
                // signal interrupt to the executing jobs
                List<JobExecutionContext> jobs = scheduler.getCurrentlyExecutingJobs();
                if (jobs != null)
                    for (JobExecutionContext j : jobs) {
                        if (j.getJobInstance() instanceof InterruptableJob) {
                            ((InterruptableJob)j.getJobInstance()).interrupt();
                        }
                    }
                // wait for concurrently executing Jobs to finish
                while (!(scheduler.getCurrentlyExecutingJobs().isEmpty())) {
                    logger.info(String.format("%d jobs executing,  waiting for them to complete...", 
                            scheduler.getCurrentlyExecutingJobs().size()));
                    
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        logger.warn("Sleep interrupted. check again!");
                    }
                }
                logger.info("Job scheduler finish executing all jobs.");
                scheduler.deleteJob(jobKey(QUARTZ_PROCESSOR_JOB, QUARTZ_PROCESSOR_GROUP));
            }
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
